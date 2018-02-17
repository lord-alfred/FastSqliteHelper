using System;
using System.IO;
using System.Reflection;
using System.Collections;
using System.Collections.Generic;

using System.Data.SQLite;

using ZennoLab.InterfacesLibrary.ProjectModel;


namespace FastSqliteHelperLib
{
    /// <summary>
    /// Базовое исключение для класса FastSqliteHelper, чтоб было проще перехватывать и обработать
    /// </summary>
    public class FastSqliteHelperException: Exception {
        public FastSqliteHelperException() {}
        public FastSqliteHelperException(string message): base(message) {}
        public FastSqliteHelperException(string message, Exception inner): base(message, inner) {}
    }
    
    public static class FastSqliteHelper
    {
        // подключение к базе
        [ThreadStatic] public static SQLiteConnection connection;
        // объект project из Zenno (чтоб можно было отправить сообщение в лог)
        [ThreadStatic] private static IZennoPosterProjectModel project;
        // выдавать ли исключение в случае ошибки или просто по-тихому писать в лог ZP
        [ThreadStatic] private static bool throw_on_errors;
        // показывать ли ошибку в логе ZennoPoster (в случае false - сообщение будет отображено только в ProjectMaker)
        [ThreadStatic] private static bool show_in_poster;

        // тип сообщения в логе
        public enum LogType {LogInfo, LogWarning, LogError};
        
        // префикс для параметров, передаваемых в БД
        private const string param_prefix = "@param_{0}";
        // именованный префикс для параметров, передаваемых в БД
        private const string named_param_prefix = "{0} = @param_{0}";
        
        /// <summary>
        /// Генерация сообщения об ошибке с указанием метода, где что-то пошло "не так"
        /// </summary>
        /// <param name="msg">сообщение об ошибке</param>
        /// <param name="method_name">название метода, где произошла ошибка</param>
        /// <returns>полное сообщение об ошибке</returns>
        public static string GenerateErrorMessage(string msg, string method_name) {
            return String.Format("[FastSqliteHelper.{0}]: {1}", method_name, msg);
        }
        
        /// <summary>
        /// Отправка сообщения в лог (ZP/PM или в консоль)
        /// </summary>
        /// <param name="msg">сообщение</param>
        /// <param name="log_type">тип сообщения</param>
        public static void SendToLog(string msg, LogType log_type=LogType.LogInfo) {
            if (project != null) {
                switch(log_type) {
                    case LogType.LogInfo:
                        project.SendInfoToLog(msg, show_in_poster);
                        break;
                    
                    case LogType.LogWarning:
                        project.SendWarningToLog(msg, show_in_poster);
                        break;
                        
                    case LogType.LogError:
                        project.SendErrorToLog(msg, show_in_poster);
                        break;
                }
            } else {
                ConsoleColor previous_bgcolor = Console.BackgroundColor;
                ConsoleColor previous_fgcolor = Console.ForegroundColor;
                switch(log_type) {
                    case LogType.LogInfo:
                        Console.BackgroundColor = ConsoleColor.Black;
                        Console.ForegroundColor = ConsoleColor.White;
                        break;
                    
                    case LogType.LogWarning:
                        Console.BackgroundColor = ConsoleColor.Black;
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        break;
                        
                    case LogType.LogError:
                        Console.BackgroundColor = ConsoleColor.Black;
                        Console.ForegroundColor = ConsoleColor.Red;
                        break;
                }
                
                Console.WriteLine(msg);
                
                Console.BackgroundColor = previous_bgcolor;
                Console.ForegroundColor = previous_fgcolor;
            }
        }
        
        /// <summary>
        /// Инициализация подключения к SQLite базе данных
        /// </summary>
        /// <param name="database_path">полный путь к файлу базы данных</param>
        /// <param name="zp_project">объект project из Zenno</param>
        /// <param name="add_to_connection_string">строка дополнительных параметров, добавляемых к connection string</param>
        /// <param name="throw_exc_on_errors">выдавать ли исключение в случае ошибки или просто по-тихому писать в лог ZP</param>
        /// <param name="show_log_in_poster">показывать ли ошибку в логе ZennoPoster (в случае false - сообщение будет отображено только в ProjectMaker)</param>
        /// <returns>удалось ли подключиться к БД</returns>
        public static bool Init(string database_path, IZennoPosterProjectModel zp_project, string add_to_connection_string="", bool throw_exc_on_errors=true, bool show_log_in_poster=false) {
            project = zp_project;
            throw_on_errors = throw_exc_on_errors;
            show_in_poster = show_log_in_poster;
            
            try {
                string connection_string = string.Format("Data Source={0};{1}", database_path, add_to_connection_string);
                
                if(!File.Exists(database_path)) {
                    SQLiteConnection.CreateFile(database_path);
                }
                connection = new SQLiteConnection(connection_string);
                connection.Open();

                return true;
            } catch (Exception exc) {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage(String.Format("Ошибка подключения к БД: {0}", exc.Message), current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg, exc);
                } else {
                    SendToLog(msg, LogType.LogError);
                }

                return false;
            }
        }
        
        /// <summary>
        /// Инициализация подключения к SQLite базе данных
        /// </summary>
        /// <param name="database_path">полный путь к файлу базы данных</param>
        /// <param name="zp_project">объект project из Zenno</param>
        /// <param name="add_to_connection_string">словарь "ключ" => "значение" дополнительных параметров, добавляемых к connection string</param>
        /// <param name="throw_exc_on_errors">выдавать ли исключение в случае ошибки или просто по-тихому писать в лог ZP</param>
        /// <param name="show_log_in_poster">показывать ли ошибку в логе ZennoPoster (в случае false - сообщение будет отображено только в ProjectMaker)</param>
        /// <returns>удалось ли подключиться к БД</returns>
        public static bool Init(string database_path, IZennoPosterProjectModel zp_project, Dictionary<string, string> add_to_connection_string, bool throw_exc_on_errors=true, bool show_log_in_poster=false) {
            List<string> addition_lst = new List<string>();
            foreach(KeyValuePair<string, string> data in add_to_connection_string) {
                addition_lst.Add(String.Format("{0}={1}", data.Key, data.Value));
            }
            string addition_str = String.Join(";", addition_lst);
            
            return Init(database_path, zp_project, addition_str, throw_exc_on_errors, show_log_in_poster);
        }
        
        /// <summary>
        /// Закрытие подключения к базе данных
        /// </summary>
        public static void DeInit() {
            if (connection != null) {
                connection.Close();
                connection = null;
            } else {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage("Соединение уже закрыто/ещё не открыто", current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg);
                } else {
                    SendToLog(msg, LogType.LogError);
                }
            }
        }
        
        /// <summary>
        /// Установка параметра PRAGMA для текущего подключения к БД
        /// </summary>
        /// <param name="name_and_value">название и значение параметра PRAGMA</param>
        public static void PragmaSet(string name_and_value) {
            try {
                string sql = String.Format("PRAGMA {0};", name_and_value);
                Query(sql);
            } catch (Exception exc) {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage(String.Format("Ошибка установки Pragma: {0}", exc.Message), current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg, exc);
                } else {
                    SendToLog(msg, LogType.LogError);
                }
            }
        }
        
        /// <summary>
        /// Чтение параметра PRAGMA из текущего подключения к БД
        /// </summary>
        /// <param name="name">название параметра PRAGMA</param>
        /// <returns>значение параметра</returns>
        public static object PragmaGet(string name) {
            try {
                string sql = String.Format("PRAGMA {0};", name);
                return QueryScalar(sql);
            } catch (Exception exc) {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage(String.Format("Ошибка чтения Pragma: {0}", exc.Message), current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg, exc);
                } else {
                    SendToLog(msg, LogType.LogError);
                }
                
                return null;
            }
        }
        
        /// <summary>
        /// Получение данных из таблицы по условию
        /// </summary>
        /// <param name="table">таблица</param>
        /// <param name="columns">строка столбцов, разделенных запятой</param>
        /// <param name="condition">условие выборки</param>
        /// <returns>объект SQLiteDataReader с содержимым</returns>
        public static SQLiteDataReader Select(string table, string columns, string condition="") {
            try {
                if(!String.IsNullOrEmpty(condition)) {
                    condition = String.Format("and {0}", condition);
                }
                
                SQLiteCommand cmd = connection.CreateCommand();
                cmd.CommandText = String.Format("SELECT {0} FROM {1} WHERE 1=1 {2}", columns, table, condition);

                return cmd.ExecuteReader();
            } catch (Exception exc) {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage(String.Format("Ошибка: {0}", exc.Message), current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg, exc);
                } else {
                    SendToLog(msg, LogType.LogError);
                }

                return null;
            }
        }
        
        /// <summary>
        /// Получение данных из таблицы по условию
        /// </summary>
        /// <param name="table">таблица</param>
        /// <param name="columns">массив строк столбцов</param>
        /// <param name="condition">условие выборки</param>
        /// <returns>объект SQLiteDataReader с содержимым</returns>
        public static SQLiteDataReader Select(string table, string[] columns, string condition="") {
            string columns_string = String.Join(", ", columns);
            return Select(table, columns_string, condition);
        }
        
        /// <summary>
        /// Получение данных из таблицы по условию
        /// </summary>
        /// <param name="table">таблица</param>
        /// <param name="columns">список строк столбцов</param>
        /// <param name="condition">условие выборки</param>
        /// <returns>объект SQLiteDataReader с содержимым</returns>
        public static SQLiteDataReader Select(string table, IEnumerable columns, string condition="") {
            string columns_string = String.Join(", ", columns);
            return Select(table, columns_string, condition);
        }
        
        
        /// <summary>
        /// Добавление одной записи в таблицу
        /// </summary>
        /// <param name="table">таблица</param>
        /// <param name="data">данные в виде словаря "столбец" => "значение"</param>
        /// <returns>количество добавленных записей</returns>
        public static int Insert(string table, Dictionary<string, object> data) {
            try {
                SQLiteCommand cmd = connection.CreateCommand();
                
                List<string> data_fields = new List<string>();
                List<string> data_params = new List<string>();
                foreach(KeyValuePair<string, object> field_data in data) {
                    data_fields.Add(field_data.Key);
                    
                    string field_param = String.Format(param_prefix, field_data.Key);
                    data_params.Add(field_param);
                    cmd.Parameters.AddWithValue(field_param, field_data.Value);
                }
                string data_fields_string = String.Join(", ", data_fields);
                string data_params_string = String.Join(", ", data_params);
                
                cmd.CommandText = String.Format("INSERT INTO {0} ({1}) VALUES({2})", table, data_fields_string, data_params_string);
                
                return cmd.ExecuteNonQuery();
            } catch (Exception exc) {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage(String.Format("Ошибка: {0}", exc.Message), current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg, exc);
                } else {
                    SendToLog(msg, LogType.LogError);
                }
                
                return -1;
            }
        }
        
        /// <summary>
        /// Добавление множества записей в таблицу (используя транзакцию на все добавляемые данные)
        /// </summary>
        /// <param name="table">таблица</param>
        /// <param name="data_list">список словарей с данными в виде "столбец" => "значение"</param>
        /// <returns>количество добавленных записей</returns>
        public static int Insert(string table, List<Dictionary<string, object>> data_list) {
            try {
                int count = 0;
                SQLiteTransaction transaction = connection.BeginTransaction();
                try {
                    foreach(Dictionary<string, object> data in data_list) {
                        int current_count = Insert(table, data);
                        if (current_count > 0) {
                            count += current_count;
                        } else {
                            count = -1;
                            break;
                        }
                    }
                    if (count > 0) {
                        transaction.Commit();
                    } else {
                        transaction.Rollback();
                    }
                } catch (Exception exc) {
                    transaction.Rollback();
                    
                    throw new FastSqliteHelperException(exc.Message, exc);
                }
                
                return count;
            } catch (Exception exc) {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage(String.Format("Ошибка: {0}", exc.Message), current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg, exc);
                } else {
                    SendToLog(msg, LogType.LogError);
                }
                
                return -1;
            }
        }
        
        /// <summary>
        /// Идентификатор последней добавленной записи
        /// </summary>
        /// <returns>идентификатор</returns>
        public static long LastInsertID() {
            return connection.LastInsertRowId;
        }
        
        
        /// <summary>
        /// Обновление значений в таблице по указанному условию
        /// </summary>
        /// <param name="table">таблица</param>
        /// <param name="data">словарь "столбец" => "значение" для изменения</param>
        /// <param name="condition">условие изменения</param>
        /// <returns>успешно ли произошло обновление</returns>
        public static bool Update(string table, Dictionary<string, object> data, string condition="") {
            try {
                if(!String.IsNullOrEmpty(condition)) {
                    condition = String.Format("and {0}", condition);
                }
                
                SQLiteCommand cmd = connection.CreateCommand();
                
                List<string> data_params = new List<string>();
                foreach(KeyValuePair<string, object> field_data in data) {
                    string field_param = String.Format(param_prefix, field_data.Key);
                    cmd.Parameters.AddWithValue(field_param, field_data.Value);
                    
                    field_param = String.Format(named_param_prefix, field_data.Key); // smallhack
                    data_params.Add(field_param);
                }
                string data_params_string = String.Join(", ", data_params);
                
                cmd.CommandText = String.Format("UPDATE {0} set {1} WHERE 1=1 {2}", table, data_params_string, condition);
                int count = cmd.ExecuteNonQuery();
                
                return (count > 0);
            } catch (Exception exc) {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage(String.Format("Ошибка: {0}", exc.Message), current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg, exc);
                } else {
                    SendToLog(msg, LogType.LogError);
                }
                
                return false;
            }
        }
        
        /// <summary>
        /// Обновление значения одного столбца в таблице по указанному условию
        /// </summary>
        /// <param name="table">таблица</param>
        /// <param name="field_name">столбец</param>
        /// <param name="field_value">значение</param>
        /// <param name="condition">условие изменения</param>
        /// <returns>успешно ли произошло обновление</returns>
        public static bool Update(string table, string field_name, object field_value, string condition="") {
            Dictionary<string, object> data = new Dictionary<string, object>();
            data.Add(field_name, field_value);
            return Update(table, data, condition);
        }
        
        /// <summary>
        /// Удаление записей из таблицы по указанному условию
        /// </summary>
        /// <param name="table">таблица</param>
        /// <param name="condition">условие для удаления</param>
        /// <returns>количество удаленных записей</returns>
        public static int Delete(string table, string condition) { // not empty condition by default for prevent "remove all data from table"
            try {
                if(!String.IsNullOrEmpty(condition)) {
                    condition = String.Format("and {0}", condition);
                }
                
                SQLiteCommand cmd = connection.CreateCommand();
                cmd.CommandText = String.Format("DELETE FROM {0} WHERE 1=1 {1}", table, condition);
                
                return cmd.ExecuteNonQuery();
            } catch (Exception exc) {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage(String.Format("Ошибка: {0}", exc.Message), current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg, exc);
                } else {
                    SendToLog(msg, LogType.LogError);
                }
                
                return -1;
            }
        }
        
        /// <summary>
        /// Удаление записей из таблицы по указанному условию
        /// </summary>
        /// <param name="table">таблица</param>
        /// <param name="conditions">словарь "столбец" => "значение" для формирования условия выборки</param>
        /// <param name="logical_operator">логический оператор (AND/OR) для формирования условия</param>
        /// <returns>количество удаленных записей</returns>
        public static int Delete(string table, Dictionary<string, object> conditions, string logical_operator="AND") {
            try {
                SQLiteCommand cmd = connection.CreateCommand();
                
                // FIXME: скорее всего лучше использовать не параметры, а что-то вроде prepare value
                // https://medoo.in/api/quote / http://php.net/manual/ru/pdo.quote.php - примеры для PHP PDO
                // чтобы можно было это всё завернуть в итоге в строку и передать в предыдущий метод,
                // но это не решит проблему с тем, что нельзя задать одинаковые ключи (id=1 OR id=2)
                
                List<string> conditions_params = new List<string>();
                foreach(KeyValuePair<string, object> condition in conditions) {
                    string field_param = String.Format(param_prefix, condition.Key);
                    cmd.Parameters.AddWithValue(field_param, condition.Value);
                    
                    field_param = String.Format(named_param_prefix, condition.Key); // smallhack
                    conditions_params.Add(field_param);
                }
                string condition_string = String.Join(String.Format(" {0} ", logical_operator), conditions_params);
                
                cmd.CommandText = String.Format("DELETE FROM {0} WHERE {1}", table, condition_string);
                
                return cmd.ExecuteNonQuery();
            } catch (Exception exc) {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage(String.Format("Ошибка: {0}", exc.Message), current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg, exc);
                } else {
                    SendToLog(msg, LogType.LogError);
                }
                
                return -1;
            }
        }
        
        /// <summary>
        /// Любой запрос к БД, возвращающий количество затронутых строк
        /// </summary>
        /// <param name="sql">текст запроса</param>
        /// <returns>количество затронутых строк</returns>
        public static int Query(string sql) {
            try {
                SQLiteCommand cmd = connection.CreateCommand();
                cmd.CommandText = sql;

                return cmd.ExecuteNonQuery();
            } catch (Exception exc) {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage(String.Format("Ошибка: {0}", exc.Message), current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg, exc);
                } else {
                    SendToLog(msg, LogType.LogError);
                }

                return -1;
            }
        }
        
        /// <summary>
        /// Любой запрос к БД, возвращающий объект SQLiteDataReader с содержимым
        /// </summary>
        /// <param name="sql">текст запроса</param>
        /// <returns>объект SQLiteDataReader с содержимым</returns>
        public static SQLiteDataReader QueryReader(string sql) {
            try {
                SQLiteCommand cmd = connection.CreateCommand();
                cmd.CommandText = sql;

                return cmd.ExecuteReader();
            } catch (Exception exc) {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage(String.Format("Ошибка: {0}", exc.Message), current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg, exc);
                } else {
                    SendToLog(msg, LogType.LogError);
                }

                return null;
            }
        }
        
        /// <summary>
        /// Любой запрос к БД, возвращающий первый столбец первой строки
        /// </summary>
        /// <param name="sql">текст запроса</param>
        /// <returns>первый столбец первой строки (если вернулась хотя бы 1 строка) или null (в ином случае)</returns>
        public static object QueryScalar(string sql) {
            try {
                SQLiteCommand cmd = connection.CreateCommand();
                cmd.CommandText = sql;

                return cmd.ExecuteScalar();
            } catch (Exception exc) {
                string current_method_name = MethodBase.GetCurrentMethod().Name;
                string msg = GenerateErrorMessage(String.Format("Ошибка: {0}", exc.Message), current_method_name);
                
                if (throw_on_errors) {
                    throw new FastSqliteHelperException(msg, exc);
                } else {
                    SendToLog(msg, LogType.LogError);
                }

                return null;
            }
        }
    }
}