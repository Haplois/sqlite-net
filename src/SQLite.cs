//
// Copyright (c) 2009-2012 Krueger Systems, Inc.
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
#if WINDOWS_PHONE && !USE_WP8_NATIVE_SQLITE
#define USE_CSHARP_SQLITE
#endif

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

#if USE_CSHARP_SQLITE
using Sqlite3 = Community.CsharpSqlite.Sqlite3;
using Sqlite3DatabaseHandle = Community.CsharpSqlite.Sqlite3.sqlite3;
using Sqlite3Statement = Community.CsharpSqlite.Sqlite3.Vdbe;
#elif USE_WP8_NATIVE_SQLITE
using Sqlite3 = Sqlite.Sqlite3;
using Sqlite3DatabaseHandle = Sqlite.Database;
using Sqlite3Statement = Sqlite.Statement;
#else
using Sqlite3DatabaseHandle = System.IntPtr;
using Sqlite3Statement = System.IntPtr;
using System.Text;
#endif

namespace SQLite
{
    public static class ReflectionHelper
    {
        public static IDictionary<string, object> ToExpando(object anonymousObject)
        {
            IDictionary<string, object> expando = null;

            if (anonymousObject != null)
            {
                if (anonymousObject is IDictionary<string, object>)
                    expando = anonymousObject as IDictionary<string, object>;
                else
                {
#if NETFX_CORE
                    expando = new ExpandoObject();
                    foreach (var property in GetProperties(anonymousObject))
                        expando.Add(property.Name, property.GetValue(anonymousObject));
#else
                    expando = new Dictionary<string, object>();
                    foreach (var property in GetProperties(anonymousObject))
                        expando.Add(property.Name, property.GetValue(anonymousObject, null));
#endif
                }
            }

            return expando;
        } 

        public static IDictionary<string, string> ToExpandoString(object anonymousObject)
        {
            var exp = ToExpando(anonymousObject);

            IDictionary<string, string> expando = new Dictionary<string, string>();

            foreach (var item in exp)
                expando.Add(item.Key, item.Value.ToString());

            return expando;
        }

        public static IList<string> ToExpandoKeys(object anonymousObject)
        {
            List<string> keys = new List<string>();

            if (anonymousObject != null)
            {
                foreach (var property in GetProperties(anonymousObject))
                {
                    keys.Add(property.Name);
                }
            }

            return keys;
        }

        public static IEnumerable<PropertyInfo> GetProperties(object o)
        {
            if (o == null) return null;

#if NETFX_CORE
            var type = o.GetType();

            return type.GetTypeInfo().DeclaredProperties;
#else
            return o.GetType().GetProperties(BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public);
#endif
        }
    }
#if !NETFX_CORE
    public static class ReflectionExtensions
    {
        public static T[] GetCustomAttributes<T>(this MemberInfo member, bool inherit) where T : Attribute
        {
            return member.GetCustomAttributes(typeof(T), inherit).Cast<T>().ToArray();
        }

        public static T GetCustomAttribute<T>(this MemberInfo member, bool inherit) where T : Attribute
        {
            return (T)member.GetCustomAttributes(typeof(T), inherit).SingleOrDefault();
        }

        public static object GetValue(this PropertyInfo prop, string name)
        {
            return prop.GetValue(name, null);
        }
    }

    public static class StringExtensions
    {
        public static bool IsNullOrWhitespace(this string _, string s)
        {
            return false;
        }

        public static string Join(this string _, string separator, IEnumerable<object> value)
        {
            return string.Join(separator, value.Select(i => i.ToString()).ToArray());
        }
    }
#endif

    public class SQLiteException : Exception
    {
        public SQLite3.Result Result { get; private set; }

        protected SQLiteException(SQLite3.Result r, string message) : base(message)
        {
            Result = r;
        }

        public static SQLiteException New(SQLite3.Result r, string message)
        {
            return new SQLiteException(r, message);
        }
    }

    [Flags]
    public enum SQLiteOpenFlags
    {
        ReadOnly = 1, ReadWrite = 2, Create = 4,
        NoMutex = 0x8000, FullMutex = 0x10000,
        SharedCache = 0x20000, PrivateCache = 0x40000,
        ProtectionComplete = 0x00100000,
        ProtectionCompleteUnlessOpen = 0x00200000,
        ProtectionCompleteUntilFirstUserAuthentication = 0x00300000,
        ProtectionNone = 0x00400000
    }

    [Flags]
    public enum CreateFlags
    {
        None = 0,
        ImplicitPK = 1,    // create a primary key for field called 'Id' (Orm.ImplicitPkName)
        ImplicitIndex = 2, // create an index for fields ending in 'Id' (Orm.ImplicitIndexSuffix)
        AllImplicit = 3,   // do both above

        AutoIncPK = 4      // force PK field to be auto inc
    }

    /// <summary>
    /// Represents an open connection to a SQLite database.
    /// </summary>
    public partial class SQLiteConnection : IDisposable
    {
        private bool _open;
        private TimeSpan _busyTimeout;
        private Dictionary<string, TableMapping> _mappings = null;
        private Dictionary<string, TableMapping> _tables = null;
        private System.Diagnostics.Stopwatch _sw;
        private long _elapsedMilliseconds = 0;

        private int _transactionDepth = 0;
        private Random _rand = new Random();

        public Sqlite3DatabaseHandle Handle { get; private set; }
        public bool IsOpen { get { return _open; } }
        internal static readonly Sqlite3DatabaseHandle NullHandle = default(Sqlite3DatabaseHandle);

        public string DatabasePath { get; private set; }

        public bool TimeExecution { get; set; }

        public bool Trace { get; set; }

        public bool StoreDateTimeAsTicks { get; private set; }

        /// <summary>
        /// Constructs a new SQLiteConnection and opens a SQLite database specified by databasePath.
        /// </summary>
        /// <param name="databasePath">
        /// Specifies the path to the database file.
        /// </param>
        /// <param name="storeDateTimeAsTicks">
        /// Specifies whether to store DateTime properties as ticks (true) or strings (false). You
        /// absolutely do want to store them as Ticks in all new projects. The default of false is
        /// only here for backwards compatibility. There is a *significant* speed advantage, with no
        /// down sides, when setting storeDateTimeAsTicks = true.
        /// </param>
        public SQLiteConnection(string databasePath, bool storeDateTimeAsTicks = false)
            : this(databasePath, SQLiteOpenFlags.ReadWrite | SQLiteOpenFlags.Create, storeDateTimeAsTicks)
        {
        }

        /// <summary>
        /// Constructs a new SQLiteConnection and opens a SQLite database specified by databasePath.
        /// </summary>
        /// <param name="databasePath">
        /// Specifies the path to the database file.
        /// </param>
        /// <param name="storeDateTimeAsTicks">
        /// Specifies whether to store DateTime properties as ticks (true) or strings (false). You
        /// absolutely do want to store them as Ticks in all new projects. The default of false is
        /// only here for backwards compatibility. There is a *significant* speed advantage, with no
        /// down sides, when setting storeDateTimeAsTicks = true.
        /// </param>
        public SQLiteConnection(string databasePath, SQLiteOpenFlags openFlags, bool storeDateTimeAsTicks = false)
        {
            if (string.IsNullOrEmpty(databasePath))
                throw new ArgumentException("Must be specified", "databasePath");

            DatabasePath = databasePath;

#if NETFX_CORE
            SQLite3.SetDirectory(/*temp directory type*/2, Windows.Storage.ApplicationData.Current.TemporaryFolder.Path);

            Windows.Storage.StorageFile file;

            try
            {
                file = Windows.Storage.StorageFile.GetFileFromPathAsync(databasePath).AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch
            {
                var dirName = System.IO.Path.GetDirectoryName(databasePath);
                var dir = Windows.Storage.StorageFolder.GetFolderFromPathAsync(dirName).AsTask().ConfigureAwait(false).GetAwaiter().GetResult();

                file = dir.CreateFileAsync(System.IO.Path.GetFileName(DatabasePath), Windows.Storage.CreationCollisionOption.OpenIfExists).AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
            }


            var propertyKey = "System.FileAttributes";
            var props = file.Properties.RetrievePropertiesAsync(new[] { propertyKey }).AsTask().Result;
            if (props != null)
            {
                // UInt32 FILE_ATTRIBUTES_READONLY = 1;
                props[propertyKey] = (uint)32; //(uint)props[propertyKey] ^ FILE_ATTRIBUTES_READONLY;
                file.Properties.SavePropertiesAsync(props).AsTask().Wait();
            }
#endif

            Sqlite3DatabaseHandle handle;

#if SILVERLIGHT || USE_CSHARP_SQLITE
            var r = SQLite3.Open (databasePath, out handle, (int)openFlags, IntPtr.Zero);
#else
            // open using the byte[]
            // in the case where the path may include Unicode
            // force open to using UTF-8 using sqlite3_open_v2
            var databasePathAsBytes = GetNullTerminatedUtf8(DatabasePath);
            var r = SQLite3.Open(databasePathAsBytes, out handle, (int)openFlags, Sqlite3DatabaseHandle.Zero);
#endif

            Handle = handle;
            if (r != SQLite3.Result.OK)
            {
                throw SQLiteException.New(r, string.Format("Could not open database file: {0} ({1})", DatabasePath, r));
            }
            _open = true;

            StoreDateTimeAsTicks = storeDateTimeAsTicks;

            BusyTimeout = TimeSpan.FromSeconds(0.1);
        }

        static SQLiteConnection()
        {
#if MONO
            if (_preserveDuringLinkMagic)
            {
                var ti = new ColumnInfo();
                ti.Name = "magic";
            }
#endif
        }

        public void EnableLoadExtension(int onoff)
        {
            SQLite3.Result r = SQLite3.EnableLoadExtension(Handle, onoff);
            if (r != SQLite3.Result.OK)
            {
                string msg = SQLite3.GetErrmsg(Handle);
                throw SQLiteException.New(r, msg);
            }
        }

        static byte[] GetNullTerminatedUtf8(string s)
        {
            var utf8Length = System.Text.Encoding.UTF8.GetByteCount(s);
            var bytes = new byte[utf8Length + 1];
            utf8Length = System.Text.Encoding.UTF8.GetBytes(s, 0, s.Length, bytes, 0);
            return bytes;
        }

#if MONO
        /// <summary>
        /// Used to list some code that we want the MonoTouch linker
        /// to see, but that we never want to actually execute.
        /// </summary>
        static bool _preserveDuringLinkMagic;
#endif

        /// <summary>
        /// Sets a busy handler to sleep the specified amount of time when a table is locked.
        /// The handler will sleep multiple times until a total time of <see cref="BusyTimeout"/> has accumulated.
        /// </summary>
        public TimeSpan BusyTimeout
        {
            get { return _busyTimeout; }
            set
            {
                _busyTimeout = value;
                if (Handle != NullHandle)
                {
                    SQLite3.BusyTimeout(Handle, (int)_busyTimeout.TotalMilliseconds);
                }
            }
        }

        /// <summary>
        /// Returns the mappings from types to tables that the connection
        /// currently understands.
        /// </summary>
        public IEnumerable<TableMapping> TableMappings
        {
            get
            {
                return _tables != null ? _tables.Values : Enumerable.Empty<TableMapping>();
            }
        }

        /// <summary>
        /// Retrieves the mapping that is automatically generated for the given type.
        /// </summary>
        /// <param name="type">
        /// The type whose mapping to the database is returned.
        /// </param>         
        /// <param name="createFlags">
        /// Optional flags allowing implicit PK and indexes based on naming conventions
        /// </param>     
        /// <returns>
        /// The mapping represents the schema of the columns of the database and contains 
        /// methods to set and get properties of objects.
        /// </returns>
        public TableMapping GetMapping(Type type, CreateFlags createFlags = CreateFlags.None)
        {
            if (_mappings == null)
            {
                _mappings = new Dictionary<string, TableMapping>();
            }
            TableMapping map;
            if (!_mappings.TryGetValue(type.FullName, out map))
            {
                map = new TableMapping(type, createFlags);
                _mappings[type.FullName] = map;
            }
            return map;
        }

        /// <summary>
        /// Retrieves the mapping that is automatically generated for the given type.
        /// </summary>
        /// <returns>
        /// The mapping represents the schema of the columns of the database and contains 
        /// methods to set and get properties of objects.
        /// </returns>
        public TableMapping GetMapping<T>()
        {
            return GetMapping(typeof(T));
        }

        private struct IndexedColumn
        {
            public int Order;
            public string ColumnName;
        }

        private struct IndexInfo
        {
            public string IndexName;
            public string TableName;
            public bool Unique;
            public List<IndexedColumn> Columns;
        }

        /// <summary>
        /// Executes a "drop table" on the database.  This is non-recoverable.
        /// </summary>
        public int DropTable<T>()
        {
            var map = GetMapping(typeof(T));

            var query = string.Format("drop table if exists \"{0}\"", map.TableName);

            return Execute(query);
        }

        /// <summary>
        /// Executes a "create table if not exists" on the database. It also
        /// creates any specified indexes on the columns of the table. It uses
        /// a schema automatically generated from the specified type. You can
        /// later access this schema by calling GetMapping.
        /// </summary>
        /// <returns>
        /// The number of entries added to the database schema.
        /// </returns>
        public int CreateTable<T>(CreateFlags createFlags = CreateFlags.None)
        {
            return CreateTable(typeof(T), createFlags);
        }

        /// <summary>
        /// Executes a "create table if not exists" on the database. It also
        /// creates any specified indexes on the columns of the table. It uses
        /// a schema automatically generated from the specified type. You can
        /// later access this schema by calling GetMapping.
        /// </summary>
        /// <param name="ty">Type to reflect to a database table.</param>
        /// <param name="createFlags">Optional flags allowing implicit PK and indexes based on naming conventions.</param>  
        /// <returns>
        /// The number of entries added to the database schema.
        /// </returns>
        public int CreateTable(Type ty, CreateFlags createFlags = CreateFlags.None)
        {
            if (_tables == null)
            {
                _tables = new Dictionary<string, TableMapping>();
            }
            TableMapping map;
            if (!_tables.TryGetValue(ty.FullName, out map))
            {
                map = GetMapping(ty, createFlags);
                _tables.Add(ty.FullName, map);
            }
            var query = $"CREATE TABLE IF NOT EXISTS '{map.TableName}' (\n";

            var decls = map.Columns.Select(p => Orm.SqlDecl(p, StoreDateTimeAsTicks));
            var decl = string.Join(",\n", decls.ToArray());
            query += decl;

            var pks = map.Columns.Where(p => p.IsPK).ToArray();
            if (pks.Length > 1)
                query += $",\n\n    CONSTRAINT 'PK_{map.TableName}' PRIMARY KEY ( {string.Join(", ", pks.Select(p => p.Name + (p.IsAutoInc ? " AUTOINCREMENT" : "")).ToArray())} )";

            query += "\n)";

            var count = Execute(query);
            if (count == 0)
            {
                // Possible bug: This always seems to return 0?
                // Table already exists, migrate it
                MigrateTable(map);
            }

            var indexes = new Dictionary<string, IndexInfo>();
            foreach (var c in map.Columns)
            {
                foreach (var i in c.Indices)
                {
                    var iname = i.Name ?? map.TableName + "_" + c.Name;
                    IndexInfo iinfo;
                    if (!indexes.TryGetValue(iname, out iinfo))
                    {
                        iinfo = new IndexInfo
                        {
                            IndexName = iname,
                            TableName = map.TableName,
                            Unique = i.Unique,
                            Columns = new List<IndexedColumn>()
                        };
                        indexes.Add(iname, iinfo);
                    }

                    if (i.Unique != iinfo.Unique)
                        throw new Exception("All the columns in an index must have the same value for their Unique property");

                    iinfo.Columns.Add(new IndexedColumn
                    {
                        Order = i.Order,
                        ColumnName = c.Name
                    });
                }
            }

            foreach (var indexName in indexes.Keys)
            {
                var index = indexes[indexName];
                var columns = string.Join("\",\"", index.Columns.OrderBy(i => i.Order).Select(i => i.ColumnName).ToArray());
                count += CreateIndex(indexName, index.TableName, columns, index.Unique);
            }

            return count;
        }

        /// <summary>
        /// Creates an index for the specified table and column.
        /// </summary>
        /// <param name="indexName">Name of the index to create</param>
        /// <param name="tableName">Name of the database table</param>
        /// <param name="columnName">Name of the column to index</param>
        /// <param name="unique">Whether the index should be unique</param>
        public int CreateIndex(string indexName, string tableName, string columnName, bool unique = false)
        {
            const string sqlFormat = "create {2} index if not exists \"{3}\" on \"{0}\"(\"{1}\")";
            var sql = string.Format(sqlFormat, tableName, columnName, unique ? "unique" : "", indexName);
            return Execute(sql);
        }

        /// <summary>
        /// Creates an index for the specified table and column.
        /// </summary>
        /// <param name="tableName">Name of the database table</param>
        /// <param name="columnName">Name of the column to index</param>
        /// <param name="unique">Whether the index should be unique</param>
        public int CreateIndex(string tableName, string columnName, bool unique = false)
        {
            return CreateIndex(string.Concat(tableName, "_", columnName.Replace("\",\"", "_")), tableName, columnName, unique);
        }

        /// <summary>
        /// Creates an index for the specified object property.
        /// e.g. CreateIndex<Client>(c => c.Name);
        /// </summary>
        /// <typeparam name="T">Type to reflect to a database table.</typeparam>
        /// <param name="property">Property to index</param>
        /// <param name="unique">Whether the index should be unique</param>
        public void CreateIndex<T>(Expression<Func<T, object>> property, bool unique = false)
        {
            MemberExpression mx;
            if (property.Body.NodeType == ExpressionType.Convert)
            {
                mx = ((UnaryExpression)property.Body).Operand as MemberExpression;
            }
            else
            {
                mx = (property.Body as MemberExpression);
            }
            var propertyInfo = mx.Member as PropertyInfo;
            if (propertyInfo == null)
            {
                throw new ArgumentException("The lambda expression 'property' should point to a valid Property");
            }

            var propName = propertyInfo.Name;

            var map = GetMapping<T>();
            var colName = map.FindColumnWithPropertyName(propName).Name;

            CreateIndex(map.TableName, colName, unique);
        }

        public class ColumnInfo
        {
            [Column("name")]
            public string Name { get; set; }

            //			[Column ("type")]
            //			public string ColumnType { get; set; }

            //			public int notnull { get; set; }

            //			public string dflt_value { get; set; }

            //			public int pk { get; set; }

            public override string ToString()
            {
                return Name;
            }
        }

        public List<ColumnInfo> GetTableInfo(string tableName)
        {
            var query = "pragma table_info(\"" + tableName + "\")";
            return Query<ColumnInfo>(query);
        }

        void MigrateTable(TableMapping map)
        {
            var existingCols = GetTableInfo(map.TableName);

            var toBeAdded = new List<TableMapping.Column>();

            foreach (var p in map.Columns)
            {
                var found = false;
                foreach (var c in existingCols)
                {
                    found = (string.Compare(p.Name, c.Name, StringComparison.OrdinalIgnoreCase) == 0);
                    if (found)
                        break;
                }
                if (!found)
                {
                    toBeAdded.Add(p);
                }
            }

            foreach (var p in toBeAdded)
            {
                var addCol = "alter table \"" + map.TableName + "\" add column " + Orm.SqlDecl(p, StoreDateTimeAsTicks);
                Execute(addCol);
            }
        }

        /// <summary>
        /// Creates a new SQLiteCommand. Can be overridden to provide a sub-class.
        /// </summary>
        /// <seealso cref="SQLiteCommand.OnInstanceCreated"/>
        protected virtual SQLiteCommand NewCommand()
        {
            return new SQLiteCommand(this);
        }

        /// <summary>
        /// Creates a new SQLiteCommand given the command text with arguments.Place a 
        /// @paramName (where paramName is name of your parameter)
        /// in the command text for each of the arguments.
        /// </summary>
        /// <param name="cmdText">
        /// The fully escaped SQL.
        /// </param>
        /// <param name="ps">
        /// Parameters.
        /// </param>
        /// <returns>
        /// A <see cref="SQLiteCommand"/>
        /// </returns>
        public SQLiteCommand CreateCommand(string cmdText, IEnumerable<SQLiteParameter> ps)
        {
            if (!_open)
                throw SQLiteException.New(SQLite3.Result.Error, "Cannot create commands from unopened database");

            var cmd = NewCommand();
            cmd.CommandText = cmdText;
            if (ps != null)
                foreach (var o in ps)
                    cmd.Bind(o);

            return cmd;
        }

        public SQLiteCommand CreateCommand(string cmdText)
        {
            return CreateCommand(cmdText, null);
        }

        #region Execute
        /// <summary>
        /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a 
        /// @paramName (where paramName is name of your parameter)
        /// in the command text for each of the arguments and then executes that command.
        /// Use this method instead of Query when you don't expect rows back. Such cases include
        /// INSERTs, UPDATEs, and DELETEs.
        /// You can set the Trace or TimeExecution properties of the connection
        /// to profile execution.
        /// </summary>
        /// <param name="query">
        /// The fully escaped SQL.
        /// </param>
        public int Execute(string query)
        {
            return Execute(query, (SQLiteParameter[])null);
        }

        /// <summary>
        /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a 
        /// @paramName (where paramName is name of your parameter)
        /// in the command text for each of the arguments and then executes that command.
        /// Use this method instead of Query when you don't expect rows back. Such cases include
        /// INSERTs, UPDATEs, and DELETEs.
        /// You can set the Trace or TimeExecution properties of the connection
        /// to profile execution.
        /// </summary>
        /// <param name="query">
        /// The fully escaped SQL.
        /// </param>
        /// <param name="@params">
        /// An object which its properties to substitute for the occurences of @VVV type parameters in the query. 
        /// </param>
        /// <returns>
        /// The number of rows modified in the database as a result of this execution.
        /// </returns>
        public int Execute(string query, object parameters)
        {
            return Execute(query, ReflectionHelper.ToExpando(parameters));
        }

        /// <summary>
        /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a 
        /// @paramName (where paramName is name of your parameter)
        /// in the command text for each of the arguments and then executes that command.
        /// Use this method instead of Query when you don't expect rows back. Such cases include
        /// INSERTs, UPDATEs, and DELETEs.
        /// You can set the Trace or TimeExecution properties of the connection
        /// to profile execution.
        /// </summary>
        /// <param name="query">
        /// The fully escaped SQL.
        /// </param>
        /// <param name="parameters">
        /// Arguments to substitute for the occurences of '@NAME' in the query.
        /// </param>
        /// <returns>
        /// The number of rows modified in the database as a result of this execution.
        /// </returns>
        public int Execute(string query, IDictionary<string, object> parameters)
        {
            return Execute(query, SQLiteParameter.From(parameters));
        }

        /// <summary>
        /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a 
        /// @paramName (where paramName is name of your parameter)
        /// in the command text for each of the arguments and then executes that command.
        /// Use this method instead of Query when you don't expect rows back. Such cases include
        /// INSERTs, UPDATEs, and DELETEs.
        /// You can set the Trace or TimeExecution properties of the connection
        /// to profile execution.
        /// </summary>
        /// <param name="query">
        /// The fully escaped SQL.
        /// </param>
        /// <param name="parameters">
        /// Arguments to substitute for the occurences of '@NAME' in the query.
        /// </param>
        /// <returns>
        /// The number of rows modified in the database as a result of this execution.
        /// </returns>
        public int Execute(string query, params SQLiteParameter[] parameters)
        {
            var cmd = CreateCommand(query, parameters);

            if (TimeExecution)
            {
                if (_sw == null)
                    _sw = new Stopwatch();

                _sw.Reset();
                _sw.Start();
            }

            var r = cmd.ExecuteNonQuery();

            if (TimeExecution)
            {
                _sw.Stop();
                _elapsedMilliseconds += _sw.ElapsedMilliseconds;
                Debug.WriteLine(string.Format("Finished in {0} ms ({1:0.0} s total)", _sw.ElapsedMilliseconds, _elapsedMilliseconds / 1000.0));
            }

            return r;
        }
        #endregion

        #region ExecuteScalar
        public T ExecuteScalar<T>(string query)
        {
            return ExecuteScalar<T>(query, (SQLiteParameter[])null);
        }
        public T ExecuteScalar<T>(string query, object parameters)
        {
            return ExecuteScalar<T>(query, ReflectionHelper.ToExpando(parameters));
        }
        public T ExecuteScalar<T>(string query, Dictionary<string, object> parameters)
        {
            return ExecuteScalar<T>(query, SQLiteParameter.From(parameters));
        }
        public T ExecuteScalar<T>(string query, SQLiteParameter[] parameters)
        {
            var cmd = CreateCommand(query, parameters);

            if (TimeExecution)
            {
                if (_sw == null)
                {
                    _sw = new Stopwatch();
                }
                _sw.Reset();
                _sw.Start();
            }

            var r = cmd.ExecuteScalar<T>();

            if (TimeExecution)
            {
                _sw.Stop();
                _elapsedMilliseconds += _sw.ElapsedMilliseconds;
                Debug.WriteLine(string.Format("Finished in {0} ms ({1:0.0} s total)", _sw.ElapsedMilliseconds, _elapsedMilliseconds / 1000.0));
            }

            return r;
        }
        #endregion

        #region Query
        /// <summary>
        /// Creates a SQLiteCommand given the command text (SQL) with arguments. 
        /// It returns each row of the result using the mapping automatically generated for
        /// the given type.
        /// </summary>
        /// <param name="query">
        /// The fully escaped SQL.
        /// </param>
        /// <returns>
        /// An enumerable with one result for each row returned by the query.
        /// </returns>
        public List<T> Query<T>(string query) where T : new()
        {
            var cmd = CreateCommand(query);
            return cmd.ExecuteQuery<T>();
        }
        /// <summary>
        /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a 
        /// @paramName (where paramName is name of your parameter)'
        /// in the command text for each of the arguments and then executes that command.
        /// It returns each row of the result using the mapping automatically generated for
        /// the given type.
        /// </summary>
        /// <param name="query">
        /// The fully escaped SQL.
        /// </param>
        /// <param name="@param">
        /// Parameter(s)
        /// </param>
        /// <returns>
        /// An enumerable with one result for each row returned by the query.
        /// </returns>
        public List<T> Query<T>(string query, object parameters) where T : new()
        {
            var prm = ReflectionHelper.ToExpando(parameters);
            return Query<T>(query, prm);
        }
        public List<T> Query<T>(string query, IDictionary<string, object> parameters) where T : new()
        {
            return Query<T>(query, SQLiteParameter.From(parameters));
        }
        public List<T> Query<T>(string query, params SQLiteParameter[] parameters) where T : new()
        {
            var cmd = CreateCommand(query, parameters);
            return cmd.ExecuteQuery<T>();
        }
        #endregion

        #region DeferredQuery
        public IEnumerable<T> DeferredQuery<T>(string query) where T : new()
        {
            var cmd = CreateCommand(query);
            return cmd.ExecuteDeferredQuery<T>();
        }

        /// <summary>
        /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a 
        /// @paramName (where paramName is name of your parameter)
        /// in the command text for each of the arguments and then executes that command.
        /// It returns each row of the result using the mapping automatically generated for
        /// the given type.
        /// </summary>
        /// <param name="query">
        /// The fully escaped SQL.
        /// </param>
        /// <param name="@param">
        /// Parameter(s)
        /// </param>
        /// <returns>
        /// An enumerable with one result for each row returned by the query.
        /// The enumerator will call sqlite3_step on each call to MoveNext, so the database
        /// connection must remain open for the lifetime of the enumerator.
        /// </returns>
        public IEnumerable<T> DeferredQuery<T>(string query, object parameters) where T : new()
        {
            var prm = ReflectionHelper.ToExpando(parameters);
            return DeferredQuery<T>(query, prm);
        }

        public IEnumerable<T> DeferredQuery<T>(string query, IDictionary<string, object> parameters) where T : new()
        {
            return DeferredQuery<T>(query, SQLiteParameter.From(parameters));
        }

        public IEnumerable<T> DeferredQuery<T>(string query, params SQLiteParameter[] parameters) where T : new()
        {
            var cmd = CreateCommand(query, parameters);
            return cmd.ExecuteDeferredQuery<T>();
        }

        #endregion

        #region Mapped Query
        public List<object> Query(TableMapping map, string query)
        {
            var cmd = CreateCommand(query);
            return cmd.ExecuteQuery<object>(map);
        }
        public List<object> Query(TableMapping map, string query, object parameters)
        {
            return Query(map, query, ReflectionHelper.ToExpando(parameters));
        }
        public List<object> Query(TableMapping map, string query, IDictionary<string, object> parameters)
        {
            return Query(map, query, SQLiteParameter.From(parameters));
        }

        /// <summary>
        /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a 
        /// @paramName (where paramName is name of your parameter)
        /// in the command text for each of the arguments and then executes that command.
        /// It returns each row of the result using the specified mapping. This function is
        /// only used by libraries in order to query the database via introspection. It is
        /// normally not used.
        /// </summary>
        /// <param name="map">
        /// A <see cref="TableMapping"/> to use to convert the resulting rows
        /// into objects.
        /// </param>
        /// <param name="query">
        /// The fully escaped SQL.
        /// </param>
        /// <param name="@param">
        /// Parameter(s)
        /// </param>
        /// <returns>
        /// An enumerable with one result for each row returned by the query.
        /// </returns>
        public List<object> Query(TableMapping map, string query, params SQLiteParameter[] parameters)
        {
            var cmd = CreateCommand(query, parameters);
            return cmd.ExecuteQuery<object>(map);
        }
        #endregion

        #region Mapped DeferredQuery
        public IEnumerable<object> DeferredQuery(TableMapping map, string query)
        {
            var cmd = CreateCommand(query);
            return cmd.ExecuteDeferredQuery<object>(map);
        }

        public IEnumerable<object> DeferredQuery(TableMapping map, string query, object parameters)
        {
            return DeferredQuery(map, query, ReflectionHelper.ToExpando(parameters));
        }

        public IEnumerable<object> DeferredQuery(TableMapping map, string query, Dictionary<string, object> parameters)
        {
            return DeferredQuery(map, query, SQLiteParameter.From(parameters));
        }

        /// <summary>
        /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a 
        /// @paramName (where paramName is name of your parameter)
        /// in the command text for each of the arguments and then executes that command.
        /// It returns each row of the result using the specified mapping. This function is
        /// only used by libraries in order to query the database via introspection. It is
        /// normally not used.
        /// </summary>
        /// <param name="map">
        /// A <see cref="TableMapping"/> to use to convert the resulting rows
        /// into objects.
        /// </param>
        /// <param name="query">
        /// The fully escaped SQL.
        /// </param>
        /// <param name="@param">
        /// Parameter(s)
        /// </param>
        /// <returns>
        /// An enumerable with one result for each row returned by the query.
        /// The enumerator will call sqlite3_step on each call to MoveNext, so the database
        /// connection must remain open for the lifetime of the enumerator.
        /// </returns>
        public IEnumerable<object> DeferredQuery(TableMapping map, string query, params SQLiteParameter[] parameters)
        {
            var cmd = CreateCommand(query, parameters);
            return cmd.ExecuteDeferredQuery<object>(map);
        }
        #endregion

        /// <summary>
        /// Returns a queryable interface to the table represented by the given type.
        /// </summary>
        /// <returns>
        /// A queryable object that is able to translate Where, OrderBy, and Take
        /// queries into native SQL.
        /// </returns>
        public TableQuery<T> Table<T>() where T : new()
        {
            return new TableQuery<T>(this);
        }

        /// <summary>
        /// Attempts to retrieve an object with the given primary key from the table
        /// associated with the specified type. Use of this method requires that
        /// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
        /// </summary>
        /// <param name="pk">
        /// The plain object or <typeparamref name="IDictionary<string,object>" /> that contains primary keys.
        /// </param>
        /// <returns>
        /// The object with the given primary key. Throws a not found exception
        /// if the object is not found.
        /// </returns>
        public T Get<T>(object pk) where T : new()
        {
            var map = GetMapping(typeof(T));
            return Query<T>(map.GetByPrimaryKeySql, pk).First();
        }

        /// <summary>
        /// Attempts to retrieve the first object that matches the predicate from the table
        /// associated with the specified type. 
        /// </summary>
        /// <param name="predicate">
        /// A predicate for which object to find.
        /// </param>
        /// <returns>
        /// The object that matches the given predicate. Throws a not found exception
        /// if the object is not found.
        /// </returns>
        public T Get<T>(Expression<Func<T, bool>> predicate) where T : new()
        {
            return Table<T>().Where(predicate).First();
        }

        /// <summary>
        /// Attempts to retrieve an object with the given primary key from the table
        /// associated with the specified type. Use of this method requires that
        /// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
        /// </summary>
        /// <param name="pk">
        /// The primary key.
        /// </param>
        /// <returns>
        /// The object with the given primary key or null
        /// if the object is not found.
        /// </returns>
        public T Find<T>(object pk) where T : new()
        {
            var map = GetMapping(typeof(T));
            return Query<T>(map.GetByPrimaryKeySql, pk).FirstOrDefault();
        }

        /// <summary>
        /// Attempts to retrieve an object with the given primary key from the table
        /// associated with the specified type. Use of this method requires that
        /// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
        /// </summary>
        /// <param name="pk">
        /// The primary key.
        /// </param>
        /// <param name="map">
        /// The TableMapping used to identify the object type.
        /// </param>
        /// <returns>
        /// The object with the given primary key or null
        /// if the object is not found.
        /// </returns>
        public object Find(object pk, TableMapping map)
        {
            return Query(map, map.GetByPrimaryKeySql, pk).FirstOrDefault();
        }

        /// <summary>
        /// Attempts to retrieve the first object that matches the predicate from the table
        /// associated with the specified type. 
        /// </summary>
        /// <param name="predicate">
        /// A predicate for which object to find.
        /// </param>
        /// <returns>
        /// The object that matches the given predicate or null
        /// if the object is not found.
        /// </returns>
        public T Find<T>(Expression<Func<T, bool>> predicate) where T : new()
        {
            return Table<T>().Where(predicate).FirstOrDefault();
        }

        /// <summary>
        /// Whether <see cref="BeginTransaction"/> has been called and the database is waiting for a <see cref="Commit"/>.
        /// </summary>
        public bool IsInTransaction
        {
            get { return _transactionDepth > 0; }
        }

        /// <summary>
        /// Begins a new transaction. Call <see cref="Commit"/> to end the transaction.
        /// </summary>
        /// <example cref="System.InvalidOperationException">Throws if a transaction has already begun.</example>
        public void BeginTransaction()
        {
            // The BEGIN command only works if the transaction stack is empty, 
            //    or in other words if there are no pending transactions. 
            // If the transaction stack is not empty when the BEGIN command is invoked, 
            //    then the command fails with an error.
            // Rather than crash with an error, we will just ignore calls to BeginTransaction
            //    that would result in an error.
            if (Interlocked.CompareExchange(ref _transactionDepth, 1, 0) == 0)
            {
                try
                {
                    Execute("begin transaction");
                }
                catch (Exception ex)
                {
                    var sqlExp = ex as SQLiteException;
                    if (sqlExp != null)
                    {
                        // It is recommended that applications respond to the errors listed below 
                        //    by explicitly issuing a ROLLBACK command.
                        // TODO: This rollback failsafe should be localized to all throw sites.
                        switch (sqlExp.Result)
                        {
                            case SQLite3.Result.IOError:
                            case SQLite3.Result.Full:
                            case SQLite3.Result.Busy:
                            case SQLite3.Result.NoMem:
                            case SQLite3.Result.Interrupt:
                                RollbackTo(null, true);
                                break;
                        }
                    }
                    else
                    {
                        // Call decrement and not VolatileWrite in case we've already 
                        //    created a transaction point in SaveTransactionPoint since the catch.
                        Interlocked.Decrement(ref _transactionDepth);
                    }

                    throw;
                }
            }
            else
            {
                // Calling BeginTransaction on an already open transaction is invalid
                throw new InvalidOperationException("Cannot begin a transaction while already in a transaction.");
            }
        }

        /// <summary>
        /// Creates a savepoint in the database at the current point in the transaction timeline.
        /// Begins a new transaction if one is not in progress.
        /// 
        /// Call <see cref="RollbackTo"/> to undo transactions since the returned savepoint.
        /// Call <see cref="Release"/> to commit transactions after the savepoint returned here.
        /// Call <see cref="Commit"/> to end the transaction, committing all changes.
        /// </summary>
        /// <returns>A string naming the savepoint.</returns>
        public string SaveTransactionPoint()
        {
            int depth = Interlocked.Increment(ref _transactionDepth) - 1;
            string retVal = "S" + _rand.Next(short.MaxValue) + "D" + depth;

            try
            {
                Execute("savepoint " + retVal);
            }
            catch (Exception ex)
            {
                var sqlExp = ex as SQLiteException;
                if (sqlExp != null)
                {
                    // It is recommended that applications respond to the errors listed below 
                    //    by explicitly issuing a ROLLBACK command.
                    // TODO: This rollback failsafe should be localized to all throw sites.
                    switch (sqlExp.Result)
                    {
                        case SQLite3.Result.IOError:
                        case SQLite3.Result.Full:
                        case SQLite3.Result.Busy:
                        case SQLite3.Result.NoMem:
                        case SQLite3.Result.Interrupt:
                            RollbackTo(null, true);
                            break;
                    }
                }
                else
                {
                    Interlocked.Decrement(ref _transactionDepth);
                }

                throw;
            }

            return retVal;
        }

        /// <summary>
        /// Rolls back the transaction that was begun by <see cref="BeginTransaction"/> or <see cref="SaveTransactionPoint"/>.
        /// </summary>
        public void Rollback()
        {
            RollbackTo(null, false);
        }

        /// <summary>
        /// Rolls back the savepoint created by <see cref="BeginTransaction"/> or SaveTransactionPoint.
        /// </summary>
        /// <param name="savepoint">The name of the savepoint to roll back to, as returned by <see cref="SaveTransactionPoint"/>.  If savepoint is null or empty, this method is equivalent to a call to <see cref="Rollback"/></param>
        public void RollbackTo(string savepoint)
        {
            RollbackTo(savepoint, false);
        }

        /// <summary>
        /// Rolls back the transaction that was begun by <see cref="BeginTransaction"/>.
        /// </summary>
        /// <param name="noThrow">true to avoid throwing exceptions, false otherwise</param>
        void RollbackTo(string savepoint, bool noThrow)
        {
            // Rolling back without a TO clause rolls backs all transactions 
            //    and leaves the transaction stack empty.   
            try
            {
                if (string.IsNullOrEmpty(savepoint))
                {
                    if (Interlocked.Exchange(ref _transactionDepth, 0) > 0)
                    {
                        Execute("rollback");
                    }
                }
                else
                {
                    DoSavePointExecute(savepoint, "rollback to ");
                }
            }
            catch (SQLiteException)
            {
                if (!noThrow)
                    throw;

            }
            // No need to rollback if there are no transactions open.
        }

        /// <summary>
        /// Releases a savepoint returned from <see cref="SaveTransactionPoint"/>.  Releasing a savepoint 
        ///    makes changes since that savepoint permanent if the savepoint began the transaction,
        ///    or otherwise the changes are permanent pending a call to <see cref="Commit"/>.
        /// 
        /// The RELEASE command is like a COMMIT for a SAVEPOINT.
        /// </summary>
        /// <param name="savepoint">The name of the savepoint to release.  The string should be the result of a call to <see cref="SaveTransactionPoint"/></param>
        public void Release(string savepoint)
        {
            DoSavePointExecute(savepoint, "release ");
        }

        void DoSavePointExecute(string savepoint, string cmd)
        {
            // Validate the savepoint
            int firstLen = savepoint.IndexOf('D');
            if (firstLen >= 2 && savepoint.Length > firstLen + 1)
            {
                int depth;
                if (Int32.TryParse(savepoint.Substring(firstLen + 1), out depth))
                {
                    // TODO: Mild race here, but inescapable without locking almost everywhere.
                    if (0 <= depth && depth < _transactionDepth)
                    {
#if NETFX_CORE
                        Volatile.Write(ref _transactionDepth, depth);
#elif SILVERLIGHT
                        _transactionDepth = depth;
#else
                        Thread.VolatileWrite(ref _transactionDepth, depth);
#endif
                        Execute(cmd + savepoint);
                        return;
                    }
                }
            }

            throw new ArgumentException("savePoint is not valid, and should be the result of a call to SaveTransactionPoint.", "savePoint");
        }

        /// <summary>
        /// Commits the transaction that was begun by <see cref="BeginTransaction"/>.
        /// </summary>
        public void Commit()
        {
            if (Interlocked.Exchange(ref _transactionDepth, 0) != 0)
            {
                Execute("commit");
            }
            // Do nothing on a commit with no open transaction
        }

        /// <summary>
        /// Executes <param name="action"> within a (possibly nested) transaction by wrapping it in a SAVEPOINT. If an
        /// exception occurs the whole transaction is rolled back, not just the current savepoint. The exception
        /// is rethrown.
        /// </summary>
        /// <param name="action">
        /// The <see cref="Action"/> to perform within a transaction. <param name="action"> can contain any number
        /// of operations on the connection but should never call <see cref="BeginTransaction"/> or
        /// <see cref="Commit"/>.
        /// </param>
        public void RunInTransaction(Action action)
        {
            try
            {
                var savePoint = SaveTransactionPoint();
                action();
                Release(savePoint);
            }
            catch (Exception)
            {
                Rollback();
                throw;
            }
        }

        /// <summary>
        /// Inserts all specified objects.
        /// </summary>
        /// <param name="objects">
        /// An <see cref="IEnumerable"/> of the objects to insert.
        /// </param>
        /// <returns>
        /// The number of rows added to the table.
        /// </returns>
        public int InsertAll(System.Collections.IEnumerable objects)
        {
            var c = 0;
            RunInTransaction(() => {
                foreach (var r in objects)
                {
                    c += Insert(r);
                }
            });
            return c;
        }

        /// <summary>
        /// Inserts all specified objects.
        /// </summary>
        /// <param name="objects">
        /// An <see cref="IEnumerable"/> of the objects to insert.
        /// </param>
        /// <param name="extra">
        /// Literal SQL code that gets placed into the command. INSERT {extra} INTO ...
        /// </param>
        /// <returns>
        /// The number of rows added to the table.
        /// </returns>
        public int InsertAll(System.Collections.IEnumerable objects, string extra)
        {
            if (objects == null) return -1;

            var c = 0;
            RunInTransaction(() => {
                foreach (var r in objects)
                {
                    c += Insert(r, extra);
                }
            });
            return c;
        }

        /// <summary>
        /// Inserts all specified objects.
        /// </summary>
        /// <param name="objects">
        /// An <see cref="IEnumerable"/> of the objects to insert.
        /// </param>
        /// <param name="objType">
        /// The type of object to insert.
        /// </param>
        /// <returns>
        /// The number of rows added to the table.
        /// </returns>
        public int InsertAll(System.Collections.IEnumerable objects, Type objType)
        {
            var c = 0;
            RunInTransaction(() => {
                foreach (var r in objects)
                {
                    c += Insert(r, objType);
                }
            });
            return c;
        }

        /// <summary>
        /// Inserts the given object and retrieves its
        /// auto incremented primary key if it has one.
        /// </summary>
        /// <param name="obj">
        /// The object to insert.
        /// </param>
        /// <returns>
        /// The number of rows added to the table.
        /// </returns>
        public int Insert(object obj)
        {
            if (obj == null)
            {
                return 0;
            }
            return Insert(obj, "", obj.GetType());
        }

        /// <summary>
        /// Inserts the given object and retrieves its
        /// auto incremented primary key if it has one.
        /// If a UNIQUE constraint violation occurs with
        /// some pre-existing object, this function deletes
        /// the old object.
        /// </summary>
        /// <param name="obj">
        /// The object to insert.
        /// </param>
        /// <returns>
        /// The number of rows modified.
        /// </returns>
        public int InsertOrReplace(object obj)
        {
            if (obj == null)
            {
                return 0;
            }
            return Insert(obj, "OR REPLACE", obj.GetType());
        }

        /// <summary>
        /// Inserts the given object and retrieves its
        /// auto incremented primary key if it has one.
        /// </summary>
        /// <param name="obj">
        /// The object to insert.
        /// </param>
        /// <param name="objType">
        /// The type of object to insert.
        /// </param>
        /// <returns>
        /// The number of rows added to the table.
        /// </returns>
        public int Insert(object obj, Type objType)
        {
            return Insert(obj, "", objType);
        }

        /// <summary>
        /// Inserts the given object and retrieves its
        /// auto incremented primary key if it has one.
        /// If a UNIQUE constraint violation occurs with
        /// some pre-existing object, this function deletes
        /// the old object.
        /// </summary>
        /// <param name="obj">
        /// The object to insert.
        /// </param>
        /// <param name="objType">
        /// The type of object to insert.
        /// </param>
        /// <returns>
        /// The number of rows modified.
        /// </returns>
        public int InsertOrReplace(object obj, Type objType)
        {
            return Insert(obj, "OR REPLACE", objType);
        }

        /// <summary>
        /// Inserts the given object and retrieves its
        /// auto incremented primary key if it has one.
        /// </summary>
        /// <param name="obj">
        /// The object to insert.
        /// </param>
        /// <param name="extra">
        /// Literal SQL code that gets placed into the command. INSERT {extra} INTO ...
        /// </param>
        /// <returns>
        /// The number of rows added to the table.
        /// </returns>
        public int Insert(object obj, string extra)
        {
            if (obj == null)
            {
                return 0;
            }
            return Insert(obj, extra, obj.GetType());
        }

        /// <summary>
        /// Inserts the given object and retrieves its
        /// auto incremented primary key if it has one.
        /// </summary>
        /// <param name="obj">
        /// The object to insert.
        /// </param>
        /// <param name="extra">
        /// Literal SQL code that gets placed into the command. INSERT {extra} INTO ...
        /// </param>
        /// <param name="objType">
        /// The type of object to insert.
        /// </param>
        /// <returns>
        /// The number of rows added to the table.
        /// </returns>
        public int Insert(object obj, string extra, Type objType)
        {
            if (obj == null || objType == null)
            {
                return 0;
            }


            var map = GetMapping(objType);

#if NETFX_CORE
            if (map.PrimaryKeys != null && map.PrimaryKeys.Any(i => i.IsAutoGuid))
            {
                // no GetProperty so search our way up the inheritance chain till we find it

                foreach (var pk in map.PrimaryKeys.Where(i => i.IsAutoGuid))
                {
                    PropertyInfo prop;
                    while (objType != null)
                    {
                        var info = objType.GetTypeInfo();
                        prop = info.GetDeclaredProperty(pk.PropertyName);
                        if (prop != null)
                        {
                            if (prop.GetValue(obj, null).Equals(Guid.Empty))
                            {
                                prop.SetValue(obj, Guid.NewGuid(), null);
                            }
                            break;
                        }

                        objType = info.BaseType;
                    }
                }
            }
#else
            if (map.PrimaryKeys != null && map.PrimaryKeys.Any(i => i.IsAutoGuid))
            {
                foreach (var pk in map.PrimaryKeys.Where(i => i.IsAutoGuid))
                {
                    var prop = objType.GetProperty(pk.PropertyName);
                    if (prop != null)
                    {
                        if (prop.GetValue(obj, null).Equals(Guid.Empty))
                        {
                            prop.SetValue(obj, Guid.NewGuid(), null);
                        }
                    }
                }
            }
#endif


            var replacing = string.Compare(extra, "OR REPLACE", StringComparison.OrdinalIgnoreCase) == 0;

            var cols = replacing ? map.InsertOrReplaceColumns : map.InsertColumns;
            var expando = ReflectionHelper.ToExpando(obj);
            var parameters = cols.Select(i => new SQLiteParameter(i.Name, expando[i.Name])).ToArray();

            var insertCmd = map.GetInsertCommand(this, extra, parameters);
            var count = insertCmd.ExecuteNonQuery(parameters);

            if (map.HasAutoIncrementPrimaryKey)
            {
                var id = SQLite3.LastInsertRowid(Handle);
                map.SetAutoIncPK(obj, id);
            }

            return count;
        }

        /// <summary>
        /// Updates all of the columns of a table using the specified object
        /// except for its primary key.
        /// The object is required to have a primary key.
        /// </summary>
        /// <param name="obj">
        /// The object to update. It must have a primary key designated using the PrimaryKeyAttribute.
        /// </param>
        /// <returns>
        /// The number of rows updated.
        /// </returns>
        public int Update(object obj)
        {
            if (obj == null)
            {
                return 0;
            }
            return Update(obj, obj.GetType());
        }

        /// <summary>
        /// Updates all of the columns of a table using the specified object
        /// except for its primary key.
        /// The object is required to have a primary key.
        /// </summary>
        /// <param name="obj">
        /// The object to update. It must have a primary key designated using the PrimaryKeyAttribute.
        /// </param>
        /// <param name="objType">
        /// The type of object to insert.
        /// </param>
        /// <returns>
        /// The number of rows updated.
        /// </returns>
        public int Update(object obj, Type objType)
        {
            if (obj == null || objType == null)
            {
                return 0;
            }

            var map = GetMapping(objType);
            if (!map.HasPrimaryKey)
                throw new NotSupportedException("Cannot update " + map.TableName + ": it has no PK");

            var cols = map.Columns.Except(map.PrimaryKeys);

            string query = $"UPDATE \"{map.TableName}\" SET\n"
                + string.Join(",    \n", cols.Select(i => $"{i.Name} = @{i.Name}").ToArray())
                + Orm.GetWhereStatement(map.PrimaryKeys);

            return Execute(query, obj);
        }

        /// <summary>
        /// Updates all specified objects.
        /// </summary>
        /// <param name="objects">
        /// An <see cref="IEnumerable"/> of the objects to insert.
        /// </param>
        /// <returns>
        /// The number of rows modified.
        /// </returns>
        public int UpdateAll(System.Collections.IEnumerable objects)
        {
            var c = 0;
            RunInTransaction(() => {
                foreach (var r in objects)
                {
                    c += Update(r);
                }
            });
            return c;
        }

        /// <summary>
        /// Deletes the given object from the database using its primary key.
        /// </summary>
        /// <param name="objectToDelete">
        /// The object to delete. It must have a primary key designated using the PrimaryKeyAttribute.
        /// </param>
        /// <returns>
        /// The number of rows deleted.
        /// </returns>
        public int Delete(object objectToDelete)
        {
            var map = GetMapping(objectToDelete.GetType());
            var pk = map.PrimaryKeys;
            if (pk == null)
            {
                throw new NotSupportedException("Cannot delete " + map.TableName + ": it has no PrimaryKeys");
            }
            //var q = string.Format("DELETE FROM \"{0}\" where \"{1}\" = ?", map.TableName, pk.Name);

            var query = $"DELETE FROM {map.TableName}" + Orm.GetWhereStatement(map.PrimaryKeys);
            return Execute(query, objectToDelete);
        }

        /// <summary>
        /// Deletes the object with the specified primary key.
        /// </summary>
        /// <param name="primaryKey">
        /// The primary key of the object to delete.
        /// </param>
        /// <returns>
        /// The number of objects deleted.
        /// </returns>
        /// <typeparam name='T'>
        /// The type of object.
        /// </typeparam>
        public int Delete<T>(object primaryKey)
        {
            var map = GetMapping(typeof(T));
            var pk = map.PrimaryKeys;
            if (pk == null || pk.Length != 1)
                throw new NotSupportedException("Cannot delete " + map.TableName + ": it has no PK or more then one PKs. PK count: " + (pk?.Length ?? 0).ToString());

            var query = $"DELETE FROM {map.TableName}" + Orm.GetWhereStatement(map.PrimaryKeys);

            return Execute(query, primaryKey);
        }

        /// <summary>
        /// Deletes all the objects from the specified table.
        /// WARNING WARNING: Let me repeat. It deletes ALL the objects from the
        /// specified table. Do you really want to do that?
        /// </summary>
        /// <returns>
        /// The number of objects deleted.
        /// </returns>
        /// <typeparam name='T'>
        /// The type of objects to delete.
        /// </typeparam>
        public int DeleteAll<T>()
        {
            var map = GetMapping(typeof(T));
            var query = string.Format("delete from \"{0}\"", map.TableName);
            return Execute(query);
        }

        ~SQLiteConnection()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            Close();
        }

        public void Close()
        {
            if (_open && Handle != NullHandle)
            {
                try
                {
                    if (_mappings != null)
                    {
                        foreach (var sqlInsertCommand in _mappings.Values)
                        {
                            sqlInsertCommand.Dispose();
                        }
                        _mappings.Clear();
                        _mappings = null;
                    }

                    var r = SQLite3.Close(Handle);
                    if (r != SQLite3.Result.OK)
                    {
                        string msg = SQLite3.GetErrmsg(Handle);
                        throw SQLiteException.New(r, msg);
                    }
                }
                finally
                {
                    Handle = NullHandle;
                    _open = false;
                }
            }
        }
    }

    /// <summary>
    /// Represents a parsed connection string.
    /// </summary>
    class SQLiteConnectionString
    {
        public string ConnectionString { get; private set; }
        public string DatabasePath { get; private set; }
        public bool StoreDateTimeAsTicks { get; private set; }

#if NETFX_CORE
        static readonly string MetroStyleDataPath = Windows.Storage.ApplicationData.Current.LocalFolder.Path;
#endif

        public SQLiteConnectionString(string databasePath, bool storeDateTimeAsTicks)
        {
            ConnectionString = databasePath;
            StoreDateTimeAsTicks = storeDateTimeAsTicks;

#if NETFX_CORE
            DatabasePath = System.IO.Path.Combine(MetroStyleDataPath, databasePath);
#else
            DatabasePath = databasePath;
#endif
        }
    }

    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        public string Name { get; set; }

        public TableAttribute(string name)
        {
            Name = name;
        }

        public TableAttribute()
        {
            Name = null;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        public string Name { get; set; }

        public ColumnAttribute(string name)
        {
            Name = name;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class NotNullAttribute : Attribute
    {
        public bool IsNullable { get; set; }

        public NotNullAttribute() : this(false)
        {
        }

        public NotNullAttribute(bool isNullable)
        {
            IsNullable = isNullable;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class PrimaryKeyAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class AutoIncrementAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class IndexedAttribute : Attribute
    {
        public string Name { get; set; }
        public int Order { get; set; }
        public virtual bool Unique { get; set; }

        public IndexedAttribute()
        {
        }

        public IndexedAttribute(string name, int order)
        {
            Name = name;
            Order = order;
        }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Class)]
    public class IgnoreAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class UniqueAttribute : IndexedAttribute
    {
        public override bool Unique
        {
            get { return true; }
            set { /* throw?  */ }
        }
    }

    // https://www.sqlite.org/limits.html
    [Obsolete("SQLite doesn't have a max length.", true)]
    [AttributeUsage(AttributeTargets.Property)]
    public class MaxLengthAttribute : Attribute
    {
        public int? Value { get; private set; }

        public MaxLengthAttribute(int length)
        {
            Value = length;
        }

        public MaxLengthAttribute(bool max)
        {
            if (max)
                Value = null;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class CollationAttribute : Attribute
    {
        public string Value { get; private set; }

        public CollationAttribute(string collation)
        {
            Value = collation;
        }
    }

    public class TableMapping
    {
        public Type MappedType { get; private set; }

        public string TableName { get; private set; }

        public Column[] Columns { get; private set; }

        public Column[] PrimaryKeys { get; private set; }

        public string GetByPrimaryKeySql { get; private set; }

        Column[] _insertColumns;
        Column[] _insertOrReplaceColumns;

        public TableMapping(Type type, CreateFlags createFlags = CreateFlags.None)
        {
            MappedType = type;
            var realType = type;
#if NETFX_CORE
            var typeInfo = type.GetTypeInfo();
            var ignoreAttribute = typeInfo.GetCustomAttribute<IgnoreAttribute>(false);
            var tableAttr = type.GetTypeInfo().GetCustomAttribute<TableAttribute>(true);

            if (ignoreAttribute != null && string.IsNullOrWhiteSpace(tableAttr.Name))
                while ((realType = realType.GetTypeInfo().BaseType) != typeof(object))
                {
                    var attr = realType.GetTypeInfo().GetCustomAttribute<TableAttribute>();

                    if (attr != null || !string.IsNullOrWhiteSpace(attr.Name))
                    {
                        tableAttr = attr;
                        break;
                    }
                }

            //GetCustomAttribute< TableAttribute>(type.GetTypeInfo(), typeof(TableAttribute), true);
#else
            var typeInfo = type;
            var ignoreAttribute = (IgnoreAttribute)type.GetCustomAttributes(typeof(IgnoreAttribute), false).FirstOrDefault();
            var tableAttr = (TableAttribute)type.GetCustomAttributes(typeof(TableAttribute), true).FirstOrDefault();
#endif
            if (ignoreAttribute != null && tableAttr == null)
                throw new NotSupportedException($"The table {typeInfo.FullName} is ignored! You must set an TableAttribute to use it with Sqlite!");

            TableName = tableAttr != null && tableAttr.Name != null ? tableAttr.Name : realType.Name;

#if !NETFX_CORE
            var props = realType.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.SetProperty);
#else
            var props = from p in realType.GetRuntimeProperties()
                        where ((p.GetMethod != null && p.GetMethod.IsPublic) || (p.SetMethod != null && p.SetMethod.IsPublic) || (p.GetMethod != null && p.GetMethod.IsStatic) || (p.SetMethod != null && p.SetMethod.IsStatic))
                        select p;
#endif
            var cols = new List<Column>();
            foreach (var p in props)
            {
#if !NETFX_CORE
                var ignore = p.GetCustomAttributes(typeof(IgnoreAttribute), true).Length > 0;
#else
                var ignore = p.GetCustomAttributes(typeof(IgnoreAttribute), true).Count() > 0;
#endif
                if (p.CanWrite && !ignore)
                {
                    cols.Add(new Column(this, p, createFlags));
                }
            }
            Columns = cols.ToArray();

            PrimaryKeys = Columns.Where(c => c.IsPK).ToArray();
            HasAutoIncrementPrimaryKey = PrimaryKeys.Any(i => i.IsAutoInc);

            autoIncrementedPrimaryKey = PrimaryKeys.SingleOrDefault(i => i.IsAutoInc && i.IsPK);

            if (PrimaryKeys != null)
                GetByPrimaryKeySql = $"SELECT * FROM {TableName}" + Orm.GetWhereStatement(PrimaryKeys);
            else
                GetByPrimaryKeySql = null; // You should not be calling Get/Find without a PK
        }

        public bool HasAutoIncrementPrimaryKey { get; private set; }

        public void SetAutoIncPK(object obj, long id)
        {
            if (autoIncrementedPrimaryKey != null)
                autoIncrementedPrimaryKey.SetValue(obj, Convert.ChangeType(id, autoIncrementedPrimaryKey.ColumnType, null));
        }

        public Column[] InsertColumns
        {
            get
            {
                if (_insertColumns == null)
                {
                    _insertColumns = Columns.Where(c => !c.IsAutoInc).ToArray();
                }
                return _insertColumns;
            }
        }

        public Column[] InsertOrReplaceColumns
        {
            get
            {
                if (_insertOrReplaceColumns == null)
                {
                    _insertOrReplaceColumns = Columns.ToArray();
                }
                return _insertOrReplaceColumns;
            }
        }

        public bool HasPrimaryKey => PrimaryKeys != null && PrimaryKeys.Length > 0;

        public Column FindColumnWithPropertyName(string propertyName)
        {
            var exact = Columns.SingleOrDefault(c => c.PropertyName == propertyName);
            return exact;
        }

        public Column FindColumn(string columnName)
        {
            var exact = Columns.FirstOrDefault(c => c.Name == columnName);
            return exact;
        }

        PreparedSqlLiteInsertCommand _insertCommand;
        string _insertCommandExtra;
        private Column autoIncrementedPrimaryKey;

        public PreparedSqlLiteInsertCommand GetInsertCommand(SQLiteConnection conn, string extra, SQLiteParameter[] parameters)
        {
            if (_insertCommand == null)
            {
                _insertCommand = CreateInsertCommand(conn, extra, parameters);
                _insertCommandExtra = extra;
            }
            else if (_insertCommandExtra != extra)
            {
                _insertCommand.Dispose();
                _insertCommand = CreateInsertCommand(conn, extra, parameters);
                _insertCommandExtra = extra;
            }
            return _insertCommand;
        }

        PreparedSqlLiteInsertCommand CreateInsertCommand(SQLiteConnection conn, string extra, SQLiteParameter[] parameters)
        {
            string insertSql;

            if (!parameters.Any() && !Columns.Any(i => !i.IsAutoInc))
                insertSql = $"INSERT {extra} INTO \"{TableName}\" DEFAULT VALUES";

            else
                insertSql = $"INSERT {extra} INTO \"{TableName}\" ( "
                    + string.Join(",\n    ", (from c in parameters select "\"" + c.Name + "\"").ToArray())
                    + ") VALUES ( \n"
                    + string.Join(",\n    ", (from c in parameters select c.ParameterName).ToArray())
                    + ")";

            var insertCommand = new PreparedSqlLiteInsertCommand(conn);
            insertCommand.CommandText = insertSql;
            return insertCommand;
        }

        protected internal void Dispose()
        {
            if (_insertCommand != null)
            {
                _insertCommand.Dispose();
                _insertCommand = null;
            }
        }

        public class Column : IEquatable<Column>
        {
            PropertyInfo _prop;

            public string Name { get; private set; }

            public TableMapping Table { get; }

            public string PropertyName { get { return _prop.Name; } }

            public Type ColumnType { get; private set; }

            public string Collation { get; private set; }

            public bool IsAutoInc { get; private set; }
            public bool IsAutoGuid { get; private set; }

            public bool IsPK { get; private set; }

            public IEnumerable<IndexedAttribute> Indices { get; set; }

            public bool IsNullable { get; private set; }

            public string MaxStringLength { get; private set; }
            public string TableName { get; private set; }
            public string FullyQualifiedColumnName => $"\"{TableName}\".\"{Name}\"";

            public Column(TableMapping table, PropertyInfo prop, CreateFlags createFlags = CreateFlags.None)
            {
                var columnAttribute = prop.GetCustomAttribute<ColumnAttribute>(false);
                var notNullableAttribute = prop.GetCustomAttribute<NotNullAttribute>(false);
                var forcedNullability = false;

                if (notNullableAttribute != null) forcedNullability = notNullableAttribute.IsNullable;

                _prop = prop;
                Name = columnAttribute == null ? prop.Name : columnAttribute.Name;
                Table = table;
                TableName = table.TableName;

                ColumnType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                forcedNullability = forcedNullability || ColumnType != prop.PropertyType;

                Collation = Orm.Collation(prop);

                IsPK = Orm.IsPK(prop) || (
                            ((createFlags & CreateFlags.ImplicitPK) == CreateFlags.ImplicitPK)
                            && string.Equals(prop.Name, Orm.ImplicitPkName, StringComparison.OrdinalIgnoreCase)
                        );

                var isAuto = Orm.IsAutoInc(prop) || (IsPK && ((createFlags & CreateFlags.AutoIncPK) == CreateFlags.AutoIncPK));
                IsAutoGuid = isAuto && ColumnType == typeof(Guid);
                IsAutoInc = isAuto && !IsAutoGuid;

                Indices = Orm.GetIndices(prop);
                if (!Indices.Any()
                    && !IsPK
                    && ((createFlags & CreateFlags.ImplicitIndex) == CreateFlags.ImplicitIndex)
                    && Name.EndsWith(Orm.ImplicitIndexSuffix, StringComparison.OrdinalIgnoreCase)
                    )
                {
                    Indices = new IndexedAttribute[] { new IndexedAttribute() };
                }

                if (IsPK && forcedNullability)
                    throw new ArgumentException($"Problem on column {Name} in {prop.DeclaringType.FullName}. A primary key cannot null!");
#if NETFX_CORE
                IsNullable = (!IsPK && !ColumnType.GetTypeInfo().IsValueType) || forcedNullability;
#else
                IsNullable = (!IsPK && !ColumnType.IsValueType) || forcedNullability;
#endif
                //MaxStringLength = Orm.MaxStringLength(prop);
            }

            public void SetValue(object obj, object val)
            {
                _prop.SetValue(obj, val, null);
            }

            public object GetValue(object obj)
            {
                return _prop.GetValue(obj, null);
            }

            public override bool Equals(object obj)
            {
                return obj is Column && Equals(obj as Column);
            }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }

            public bool Equals(Column other)
            {
                return other != null
                    && other.PropertyName.Equals(PropertyName, StringComparison.CurrentCultureIgnoreCase)
                    && other.TableName.Equals(TableName, StringComparison.CurrentCultureIgnoreCase);
            }
        }
    }

    public static class Orm
    {
        public const string ImplicitPkName = "Id";
        public const string ImplicitIndexSuffix = "Id";

        public static string SqlDecl(TableMapping.Column p, bool storeDateTimeAsTicks)
        {
            string decl = $"    \"{p.Name}\" {SqlType(p, storeDateTimeAsTicks)}";

            if (p.IsPK && p.Table.PrimaryKeys.Length <= 1)
                decl += " PRIMARY KEY";

            if (p.IsAutoInc && p.Table.PrimaryKeys.Length <= 1)
                decl += " AUTOINCREMENT";

            if (!p.IsNullable)
                decl += " NOT NULL";

            if (!string.IsNullOrEmpty(p.Collation))
                decl += $" COLLATE {p.Collation}";

            return decl;
        }

        public static string SqlType(TableMapping.Column p, bool storeDateTimeAsTicks)
        {
            var clrType = p.ColumnType;
            if (clrType == typeof(bool) || clrType == typeof(byte) || clrType == typeof(ushort) || clrType == typeof(sbyte) || clrType == typeof(short) || clrType == typeof(int))
            {
                return "INTEGER";
            }
            else if (clrType == typeof(uint) || clrType == typeof(long))
            {
                return "INTEGER";
            }
            else if (clrType == typeof(float) || clrType == typeof(double) || clrType == typeof(decimal))
            {
                return "FLOAT";
            }
            else if (clrType == typeof(string))
            {
                var len = p.MaxStringLength;
                return "TEXT";
            }
            else if (clrType == typeof(DateTime))
            {
                return storeDateTimeAsTicks ? "BIGINT" : "DATETIME";
            }
#if !NETFX_CORE
            else if (clrType.IsEnum)
#else
            else if (clrType.GetTypeInfo().IsEnum)
#endif
            {
                return "INTEGER";
            }
            else if (clrType == typeof(byte[]))
            {
                return "BLOB";
            }
            else if (clrType == typeof(Guid))
            {
                return "TEXT";
            }
            else
            {
                throw new NotSupportedException("Don't know about " + clrType);
            }
        }

        public static bool IsPK(MemberInfo p)
        {
            return p.GetCustomAttributes(typeof(PrimaryKeyAttribute), false).Any();

        }

        public static string Collation(MemberInfo p)
        {
            var attrs = p.GetCustomAttributes<CollationAttribute>(false).SingleOrDefault();

            return attrs?.Value ?? string.Empty;
        }

        public static bool IsAutoInc(MemberInfo p)
        {
            return p.GetCustomAttributes(typeof(AutoIncrementAttribute), false).Any();
        }

        public static IEnumerable<IndexedAttribute> GetIndices(MemberInfo p)
        {
            return p.GetCustomAttributes<IndexedAttribute>(false);
        }

        public static string GetWhereStatement(IEnumerable<TableMapping.Column> columns)
        {
            return " WHERE " + string.Join(" AND ", columns.Select(i => $"{i.FullyQualifiedColumnName} = @{i.PropertyName}").ToArray());
        }
    }

    public partial class SQLiteParameter : IEquatable<SQLiteParameter>
    {
        public SQLiteParameter() { }
        public SQLiteParameter(string name, object value)
        {
            Name = name;
            Value = value;
        }

        public string Name { get; set; }
        public object Value { get; set; }

        public string ParameterName
        {
            get
            {
                return $"@{Name}";
            }
        }



        internal KeyValuePair<string, object> ToKeyValuePair()
        {
            return new KeyValuePair<string, object>(Name, Value);
        }

        internal static SQLiteParameter GetUnique(object value, IEnumerable<SQLiteParameter> @params)
        {
            var index = 1;
            var param = new SQLiteParameter("param0", value);

            while (@params.Any(i => i.Name == param.Name))
                param.Name = $"param{++index}";

            return param;
        }

        public bool Equals(SQLiteParameter other)
        {
            return other != null
                && other.ParameterName == ParameterName;
        }

        public override bool Equals(object obj)
        {
            return obj != null
                && obj is SQLiteParameter
                && Equals(obj as SQLiteParameter);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        internal static SQLiteParameter[] From(IDictionary<string, object> args)
        {
            if (args == null) return null;

            return args.Select(i => new SQLiteParameter(i.Key, i.Value)).ToArray();
        }


        internal static SQLiteParameter[] From(object args)
        {
            if (args == null) return null;

            return From(ReflectionHelper.ToExpando(args));
        }
    }

    public partial class SQLiteCommand
    {
        SQLiteConnection _conn;
        private List<SQLiteParameter> _bindings;

        public string CommandText { get; set; }

        internal SQLiteCommand(SQLiteConnection conn)
        {
            _conn = conn;
            _bindings = new List<SQLiteParameter>();
            CommandText = "";
        }

        public int ExecuteNonQuery()
        {
            if (_conn.Trace)
            {
                Debug.WriteLine("Executing: " + this);
            }

            var r = SQLite3.Result.OK;
            var stmt = Prepare();
            r = SQLite3.Step(stmt);
            Finalize(stmt);
            if (r == SQLite3.Result.Done)
            {
                int rowsAffected = SQLite3.Changes(_conn.Handle);
                return rowsAffected;
            }
            else if (r == SQLite3.Result.Error)
            {
                string msg = SQLite3.GetErrmsg(_conn.Handle);
                throw SQLiteException.New(r, msg);
            }
            else
            {
                throw SQLiteException.New(r, r.ToString());
            }
        }

        public IEnumerable<T> ExecuteDeferredQuery<T>()
        {
            return ExecuteDeferredQuery<T>(_conn.GetMapping(typeof(T)));
        }

        public List<T> ExecuteQuery<T>()
        {
            return ExecuteDeferredQuery<T>(_conn.GetMapping(typeof(T))).ToList();
        }

        public List<T> ExecuteQuery<T>(TableMapping map)
        {
            return ExecuteDeferredQuery<T>(map).ToList();
        }

        /// <summary>
        /// Invoked every time an instance is loaded from the database.
        /// </summary>
        /// <param name='obj'>
        /// The newly created object.
        /// </param>
        /// <remarks>
        /// This can be overridden in combination with the <see cref="SQLiteConnection.NewCommand"/>
        /// method to hook into the life-cycle of objects.
        ///
        /// Type safety is not possible because MonoTouch does not support virtual generic methods.
        /// </remarks>
        protected virtual void OnInstanceCreated(object obj)
        {
            // Can be overridden.
        }

        public IEnumerable<T> ExecuteDeferredQuery<T>(TableMapping map)
        {
            if (_conn.Trace)
            {
                Debug.WriteLine("Executing Query: " + this);
            }

            var stmt = Prepare();
            try
            {
                var cols = new TableMapping.Column[SQLite3.ColumnCount(stmt)];

                for (int i = 0; i < cols.Length; i++)
                {
                    var name = SQLite3.ColumnName16(stmt, i);
                    cols[i] = map.FindColumn(name);
                }

                while (SQLite3.Step(stmt) == SQLite3.Result.Row)
                {
                    var obj = Activator.CreateInstance(map.MappedType);
                    for (int i = 0; i < cols.Length; i++)
                    {
                        if (cols[i] == null)
                            continue;
                        var colType = SQLite3.ColumnType(stmt, i);
                        var val = ReadCol(stmt, i, colType, cols[i].ColumnType);
                        cols[i].SetValue(obj, val);
                    }
                    OnInstanceCreated(obj);
                    yield return (T)obj;
                }
            }
            finally
            {
                SQLite3.Finalize(stmt);
            }
        }

        public T ExecuteScalar<T>()
        {
            if (_conn.Trace)
            {
                Debug.WriteLine("Executing Query: " + this);
            }

            T val = default(T);

            var stmt = Prepare();

            try
            {
                var r = SQLite3.Step(stmt);
                if (r == SQLite3.Result.Row)
                {
                    var colType = SQLite3.ColumnType(stmt, 0);
                    val = (T)ReadCol(stmt, 0, colType, typeof(T));
                }
                else if (r == SQLite3.Result.Done)
                {
                }
                else
                {
                    throw SQLiteException.New(r, SQLite3.GetErrmsg(_conn.Handle));
                }
            }
            finally
            {
                Finalize(stmt);
            }

            return val;
        }


        public override string ToString()
        {
            var parts = new string[1 + _bindings.Count];
            parts[0] = CommandText;
            var i = 1;
            foreach (var b in _bindings)
            {
                parts[i] = string.Format("  {0}: {1}", i - 1, b.Value);
                i++;
            }
            return string.Join(Environment.NewLine, parts);
        }

        Sqlite3Statement Prepare()
        {
            var stmt = SQLite3.Prepare2(_conn.Handle, CommandText);
            BindAll(stmt);
            return stmt;
        }

        void Finalize(Sqlite3Statement stmt)
        {
            SQLite3.Finalize(stmt);
        }

        void BindAll(Sqlite3Statement stmt)
        {
            foreach (var b in _bindings)
                BindParameter(stmt, b, _conn.StoreDateTimeAsTicks);
        }

        internal static Sqlite3DatabaseHandle NegativePointer = new Sqlite3DatabaseHandle(-1);

        internal static void BindParameter(Sqlite3Statement stmt, SQLiteParameter parameter, bool storeDateTimeAsTicks)
        {
            var index = SQLite3.BindParameterIndex(stmt, parameter.ParameterName);
            var value = parameter.Value;

            if (value == null)
            {
                SQLite3.BindNull(stmt, index);
            }
            else
            {
                if (value is Int32)
                {
                    SQLite3.BindInt(stmt, index, (int)value);
                }
                else if (value is string)
                {
                    SQLite3.BindText(stmt, index, (string)value, -1, NegativePointer);
                }
                else if (value is byte || value is ushort || value is sbyte || value is short)
                {
                    SQLite3.BindInt(stmt, index, Convert.ToInt32(value));
                }
                else if (value is bool)
                {
                    SQLite3.BindInt(stmt, index, (bool)value ? 1 : 0);
                }
                else if (value is uint || value is long)
                {
                    SQLite3.BindInt64(stmt, index, Convert.ToInt64(value));
                }
                else if (value is float || value is double || value is decimal)
                {
                    SQLite3.BindDouble(stmt, index, Convert.ToDouble(value));
                }
                else if (value is DateTime)
                {
                    if (storeDateTimeAsTicks)
                    {
                        SQLite3.BindInt64(stmt, index, ((DateTime)value).Ticks);
                    }
                    else
                    {
                        SQLite3.BindText(stmt, index, ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss"), -1, NegativePointer);
                    }
#if !NETFX_CORE
                }
                else if (value.GetType().IsEnum)
                {
#else
                }
                else if (value.GetType().GetTypeInfo().IsEnum)
                {
#endif
                    SQLite3.BindInt(stmt, index, Convert.ToInt32(value));
                }
                else if (value is byte[])
                {
                    SQLite3.BindBlob(stmt, index, (byte[])value, ((byte[])value).Length, NegativePointer);
                }
                else if (value is Guid)
                {
                    SQLite3.BindText(stmt, index, ((Guid)value).ToString(), 72, NegativePointer);
                }
                else
                {
                    throw new NotSupportedException("Cannot store type: " + value.GetType());
                }
            }
        }

        class Binding : SQLiteParameter
        {
            public int Index { get; set; }
        }

        object ReadCol(Sqlite3Statement stmt, int index, SQLite3.ColType type, Type clrType)
        {
            if (type == SQLite3.ColType.Null)
            {
                return null;
            }
            else
            {
                if (clrType == typeof(string))
                {
                    return SQLite3.ColumnString(stmt, index);
                }
                else if (clrType == typeof(Int32))
                {
                    return (int)SQLite3.ColumnInt(stmt, index);
                }
                else if (clrType == typeof(bool))
                {
                    return SQLite3.ColumnInt(stmt, index) == 1;
                }
                else if (clrType == typeof(double))
                {
                    return SQLite3.ColumnDouble(stmt, index);
                }
                else if (clrType == typeof(float))
                {
                    return (float)SQLite3.ColumnDouble(stmt, index);
                }
                else if (clrType == typeof(DateTime))
                {
                    if (_conn.StoreDateTimeAsTicks)
                    {
                        return new DateTime(SQLite3.ColumnInt64(stmt, index));
                    }
                    else
                    {
                        var text = SQLite3.ColumnString(stmt, index);
                        return DateTime.Parse(text);
                    }
#if !NETFX_CORE
                }
                else if (clrType.IsEnum)
                {
#else
                }
                else if (clrType.GetTypeInfo().IsEnum)
                {
#endif
                    return SQLite3.ColumnInt(stmt, index);
                }
                else if (clrType == typeof(long))
                {
                    return SQLite3.ColumnInt64(stmt, index);
                }
                else if (clrType == typeof(uint))
                {
                    return (uint)SQLite3.ColumnInt64(stmt, index);
                }
                else if (clrType == typeof(decimal))
                {
                    return (decimal)SQLite3.ColumnDouble(stmt, index);
                }
                else if (clrType == typeof(byte))
                {
                    return (byte)SQLite3.ColumnInt(stmt, index);
                }
                else if (clrType == typeof(ushort))
                {
                    return (ushort)SQLite3.ColumnInt(stmt, index);
                }
                else if (clrType == typeof(short))
                {
                    return (short)SQLite3.ColumnInt(stmt, index);
                }
                else if (clrType == typeof(sbyte))
                {
                    return (sbyte)SQLite3.ColumnInt(stmt, index);
                }
                else if (clrType == typeof(byte[]))
                {
                    return SQLite3.ColumnByteArray(stmt, index);
                }
                else if (clrType == typeof(Guid))
                {
                    var text = SQLite3.ColumnString(stmt, index);
                    return new Guid(text);
                }
                else
                {
                    throw new NotSupportedException("Don't know how to read " + clrType);
                }
            }
        }

        internal void Bind(SQLiteParameter o)
        {
            if (_bindings.Contains(o))
                _bindings.Remove(o);

            _bindings.Add(o);
        }
    }

    /// <summary>
    /// Since the insert never changed, we only need to prepare once.
    /// </summary>
    public class PreparedSqlLiteInsertCommand : IDisposable
    {
        public bool Initialized { get; set; }

        protected SQLiteConnection Connection { get; set; }

        public string CommandText { get; set; }

        protected Sqlite3Statement Statement { get; set; }
        internal static readonly Sqlite3Statement NullStatement = default(Sqlite3Statement);

        internal PreparedSqlLiteInsertCommand(SQLiteConnection conn)
        {
            Connection = conn;
        }

        public int ExecuteNonQuery(SQLiteParameter[] source)
        {
            if (Connection.Trace)
            {
                Debug.WriteLine("Executing: " + CommandText);
            }

            var r = SQLite3.Result.OK;

            if (!Initialized)
            {
                Statement = Prepare();
                Initialized = true;
            }

            //bind the values.
            if (source != null)
                for (int i = 0; i < source.Length; i++)
                    SQLiteCommand.BindParameter(Statement, source[i], Connection.StoreDateTimeAsTicks);

            r = SQLite3.Step(Statement);

            if (r == SQLite3.Result.Done)
            {
                int rowsAffected = SQLite3.Changes(Connection.Handle);
                SQLite3.Reset(Statement);
                return rowsAffected;
            }
            else if (r == SQLite3.Result.Error)
            {
                string msg = SQLite3.GetErrmsg(Connection.Handle);
                SQLite3.Reset(Statement);
                throw SQLiteException.New(r, msg);
            }
            else
            {
                SQLite3.Reset(Statement);
                throw SQLiteException.New(r, r.ToString());
            }
        }

        protected virtual Sqlite3Statement Prepare()
        {
            var stmt = SQLite3.Prepare2(Connection.Handle, CommandText);
            return stmt;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (Statement != NullStatement)
            {
                try
                {
                    SQLite3.Finalize(Statement);
                }
                finally
                {
                    Statement = NullStatement;
                    Connection = null;
                }
            }
        }

        ~PreparedSqlLiteInsertCommand()
        {
            Dispose(false);
        }
    }

    public abstract class TableQueryBase
    {
        protected TableQueryBase _joinInner;
        protected Expression _joinInnerKeySelector;
        protected TableQueryBase _joinOuter;
        protected Expression _joinOuterKeySelector;
        protected Expression _joinSelector;
        protected Expression _selector;
        protected Expression _where;
        protected List<Ordering> _orderBys;
        protected int? _limit;
        protected int? _offset;
        protected Dictionary<Type, TableQueryBase> _mappings;
        protected TableQueryBase _upperQuery;
        protected bool _deferred;

        public SQLiteConnection Connection { get; protected set; }

        public TableMapping Table { get; protected set; }

        protected class CompileResult
        {
            public CompileResult(string text)
            {
                CommandText = text;
            }

            public CompileResult(string text, string paramName, object paramValue)
            {
                CommandText = text;

                if (paramName != null)
                    SetValue(paramName, paramValue);
            }

            public CompileResult(SQLiteParameter parameter)
            {
                CommandText = parameter.ParameterName;
                Parameter = parameter;
            }

            private void SetValue(string paramName, object paramValue)
            {
                Parameter = new SQLiteParameter(paramName, paramValue);
            }

            public string CommandText { get; set; }

            public SQLiteParameter Parameter { get; private set; }
        }

        protected class QueryBuilder
        {
            public string TableName { get; set; }
            public string[] Selectors { get; set; }
            public string[] Joins { get; set; }
            public string Where { get; set; }
            public string OrderBy { get; set; }
            public string LimitAndOffset { get; set; }

            public override string ToString()
            {
                var query = new StringBuilder();
                query.AppendLine("SELECT");
                query.AppendLine("    " + string.Join(",\n    ", Selectors));
                query.AppendLine($"FROM \"{TableName}\"");

                if (Joins != null && Joins.Length > 0)
                    query.AppendLine("    " + string.Join("\n    ", Joins));

                if (!string.IsNullOrEmpty(Where))
                {
                    query.AppendLine("WHERE");
                    query.AppendLine($"    {Where}");
                }

                if (!string.IsNullOrEmpty(OrderBy))
                {
                    query.AppendLine("ORDER BY");
                    query.AppendLine($"    {OrderBy}");
                }

                if (!string.IsNullOrEmpty(LimitAndOffset))
                    query.AppendLine(LimitAndOffset);

                return query.ToString();
            }
        }

        protected class Ordering
        {
            public string ColumnName { get; set; }
            public bool Ascending { get; set; }
        }

        #region Query Compilers
        protected CompileResult CompileExpr(Expression expr, IList<SQLiteParameter> @params)
        {
            if (expr == null)
            {
                throw new NotSupportedException("Expression is NULL");
            }

            else if (expr is LambdaExpression)
            {
                var la = expr as LambdaExpression;

                return CompileExpr(la.Body, @params);
            }

            else if (expr is BinaryExpression)
            {
                var bin = (BinaryExpression)expr;

                var leftr = CompileExpr(bin.Left, @params);
                var rightr = CompileExpr(bin.Right, @params);

                if (leftr == null || rightr == null) return null;

                //If either side is a parameter and is null, then handle the other side specially (for "is null"/"is not null")
                string text;
                if (leftr.CommandText.StartsWith("@") && leftr.Parameter?.Value == null)
                    text = CompileNullBinaryExpression(bin, rightr);
                else if (rightr.CommandText.StartsWith("@") && rightr.Parameter.Value == null)
                    text = CompileNullBinaryExpression(bin, leftr);
                else
                    text = "(" + leftr.CommandText + " " + GetSqlName(bin) + " " + rightr.CommandText + ")";

                return new CompileResult(text);
            }

            else if (expr.NodeType == ExpressionType.Call)
            {
                var call = (MethodCallExpression)expr;
                var args = new CompileResult[call.Arguments.Count];
                var obj = call.Object != null ? CompileExpr(call.Object, @params) : null;

                for (var i = 0; i < args.Length; i++)
                    args[i] = CompileExpr(call.Arguments[i], @params);

                var sqlCall = "";

                if (call.Method.Name == "Like" && args.Length == 2)
                {
                    sqlCall = "(" + args[0].CommandText + " LIKE " + args[1].CommandText + ")";
                }
                else if (call.Method.Name == "Contains" && args.Length == 2)
                {
                    sqlCall = "(" + args[1].CommandText + " IN " + args[0].CommandText + ")";
                }
                else if (call.Method.Name == "Contains" && args.Length == 1)
                {
                    if (call.Object != null && call.Object.Type == typeof(string))
                    {
                        sqlCall = "(" + obj.CommandText + " LIKE ('%' || " + args[0].CommandText + " || '%'))";
                    }
                    else
                    {
                        sqlCall = "(" + args[0].CommandText + " IN " + obj.CommandText + ")";
                    }
                }
                else if (call.Method.Name == "StartsWith" && args.Length == 1)
                {
                    sqlCall = "(" + obj.CommandText + " LIKE (" + args[0].CommandText + " || '%'))";
                }
                else if (call.Method.Name == "EndsWith" && args.Length == 1)
                {
                    sqlCall = "(" + obj.CommandText + " LIKE ('%' || " + args[0].CommandText + "))";
                }
                else if (call.Method.Name == "Equals" && args.Length == 1)
                {
                    sqlCall = "(" + obj.CommandText + " = (" + args[0].CommandText + "))";
                }
                else if (call.Method.Name == "ToLower")
                {
                    sqlCall = "(lower(" + obj.CommandText + "))";
                }
                else
                {
                    sqlCall = call.Method.Name.ToLower() + "(" + string.Join(",", args.Select(a => a.CommandText).ToArray()) + ")";
                }
                return new CompileResult(sqlCall);

            }
            else if (expr.NodeType == ExpressionType.Constant)
            {
                var c = (ConstantExpression)expr;

                var param = SQLiteParameter.GetUnique(c.Value, @params);
                @params.Add(param);

                return new CompileResult(param.ParameterName, param.Name, c.Value);
            }
            else if (expr.NodeType == ExpressionType.Convert)
            {
                var u = (UnaryExpression)expr;
                var ty = u.Type;
                var valr = CompileExpr(u.Operand, @params);

                if (valr == null) return null;

                return new CompileResult(valr.CommandText, valr.Parameter?.Name, valr.Parameter != null ? ConvertTo(valr.Parameter.Value, ty) : null);
            }
            else if (expr.NodeType == ExpressionType.MemberAccess)
            {
                var mem = (MemberExpression)expr;

                if (mem.Expression != null && (mem.Expression.NodeType == ExpressionType.Parameter || (_mappings?.ContainsKey(mem.Member.DeclaringType) ?? false)))
                {
                    var tableName = Table.TableName;
                    TableMapping.Column column = null;

                    if (_mappings != null)
                    {
                        var declaringType = mem.Member.DeclaringType;

                        if (_mappings.ContainsKey(declaringType))
                            column = _mappings[declaringType].Table.FindColumnWithPropertyName(mem.Member.Name);
                    }

                    if (column == null)
                        column = Table.FindColumnWithPropertyName(mem.Member.Name);

                    if (column != null)
                        return new CompileResult(column.FullyQualifiedColumnName);

                    return null;
                }
                else
                {
                    SQLiteParameter param;
                    if (mem.Expression != null)
                    {
                        var r = CompileExpr(mem.Expression, @params);
                        if (r?.Parameter == null)
                            throw new NotSupportedException("Member access failed to compile expression");

                        if (r.CommandText == r.Parameter.ParameterName)
                            @params.Remove(r.Parameter);

                        param = r.Parameter;
                    }
                    else
                        param = SQLiteParameter.GetUnique(null, @params);

                    //
                    // Get the member value
                    //
                    object val = null;

#if !NETFX_CORE
                    if (mem.Member.MemberType == MemberTypes.Property)
                    {
#else
                    if (mem.Member is PropertyInfo)
                    {
#endif
                        var m = (PropertyInfo)mem.Member;
                        val = m.GetValue(param.Value, null);
#if !NETFX_CORE
                    }
                    else if (mem.Member.MemberType == MemberTypes.Field)
                    {
#else
                    }
                    else if (mem.Member is FieldInfo)
                    {
#endif
#if SILVERLIGHT
                        val = Expression.Lambda (expr).Compile ().DynamicInvoke ();
#else
                        var m = (FieldInfo)mem.Member;
                        val = m.GetValue(param.Value);
#endif
                    }
                    else
                    {
#if !NETFX_CORE
                        throw new NotSupportedException("MemberExpr: " + mem.Member.MemberType);
#else
                        throw new NotSupportedException("MemberExpr: " + mem.Member.DeclaringType);
#endif
                    }

                    //
                    // Work special magic for enumerables
                    //
                    if (val != null && val is System.Collections.IEnumerable && !(val is string))
                    {
                        var sb = new System.Text.StringBuilder();
                        sb.Append("(");
                        var head = "";
                        foreach (var a in (System.Collections.IEnumerable)val)
                        {
                            param.Value = val;
                            @params.Add(param);
                            sb.Append(head);
                            sb.Append(param.ParameterName);
                            head = ", ";
                        }
                        sb.Append(")");
                        return new CompileResult(sb.ToString(), param.Name, val);
                    }
                    else
                    {
                        param.Value = val;
                        @params.Add(param);
                        return new CompileResult(param);
                    }
                }
            }

            throw new NotSupportedException("Cannot compile: " + expr.NodeType.ToString());
        }

        protected string CompileNullBinaryExpression(BinaryExpression expression, CompileResult parameter)
        {
            if (expression.NodeType == ExpressionType.Equal)
                return $"({parameter.CommandText} IS ?)";
            else if (expression.NodeType == ExpressionType.NotEqual)
                return $"(" + parameter.CommandText + " IS NOT ?)";
            else
                throw new NotSupportedException("Cannot compile Null-BinaryExpression with type " + expression.NodeType.ToString());
        }
        #endregion

        protected SQLiteCommand GenerateCommand(string commands = null)
        {
            var args = new List<SQLiteParameter>();
            var query = GenerateQueryBuilder(args, commands).ToString();
            return Connection.CreateCommand(query.ToString(), args);
        }

        private QueryBuilder GenerateQueryBuilder(IList<SQLiteParameter> args, string commands = null)
        {
            if (_joinInner == null && _joinOuter == null)
                return GenerateBasicQueryBuilder(args, commands);

            var mappings = new Dictionary<Type, TableQueryBase>();
            var queires = new List<TableQueryBase>();
            var coi = this;

            var tableName = getMappings(ref mappings, this);

            var qs = coi.GenerateBasicQueryBuilder(args, commands);
            qs.TableName = tableName;

            if (string.IsNullOrEmpty(commands))
                qs.Selectors = BuildSelectors(args);
            else
                qs.Selectors = new[] { commands };

            qs.Joins = BuildJoins(args);

            return qs;
        }

        protected string getMappings(ref Dictionary<Type, TableQueryBase> mappings, TableQueryBase tableQueryBase)
        {
            if (tableQueryBase == null) return null;
            if (mappings == null) mappings = new Dictionary<Type, TableQueryBase>();

            tableQueryBase._mappings = mappings;

            var inner = tableQueryBase._joinInner;
            var outer = tableQueryBase._joinOuter;

            if (outer != null && outer._joinOuter == null)
            {
                if (!mappings.ContainsKey(outer.Table.MappedType))
                    mappings.Add(outer.Table.MappedType, outer);

            }

            if (inner != null && !mappings.ContainsKey(inner.Table.MappedType))
                mappings.Add(inner.Table.MappedType, inner);

            return getMappings(ref mappings, tableQueryBase._joinOuter) ?? tableQueryBase.Table.TableName;
        }

        private QueryBuilder GenerateBasicQueryBuilder(IList<SQLiteParameter> args, string commands)
        {
            var qs = new QueryBuilder();

            qs.TableName = Table.TableName;
            qs.Where = BuildWhere(args);
            qs.OrderBy = BuildOrderBy(args);
            qs.LimitAndOffset = BuildLimitAndOffset(args);

            if (string.IsNullOrEmpty(commands))
                qs.Selectors = BuildSelectors(args);
            else
                qs.Selectors = new[] { commands };

            return qs;
        }

        #region Query builders
        private string[] BuildSelectors(IList<SQLiteParameter> args)
        {
            if (_selector == null)
                return new[] { "*" };

            var s = _selector as LambdaExpression;
            if (s == null)
                throw new NotSupportedException("These type of expressions are not supported for selection:" + _selector.NodeType.ToString());

            var b = s.Body as NewExpression;
            if (b == null)
                throw new NotSupportedException("These type of expressions are not supported for selection:" + _selector.NodeType.ToString());

            return b.Arguments.Select(i => CompileExpr(i, args)?.CommandText).Where(i => i != null).ToArray();

        }


        private string[] BuildJoins(IList<SQLiteParameter> args)
        {
            if (_mappings == null && _mappings.Count == 0) return null;
            var joins = new List<string>(_mappings.Count);
            var m = _mappings.SingleOrDefault(i => i.Value._upperQuery != null).Value;
            if (m == null) return null;

            do
            {
                if (m._joinInnerKeySelector == null || m._joinOuterKeySelector == null) continue;

                var l = CompileExpr(m._joinInnerKeySelector, args);
                var r = CompileExpr(m._joinOuterKeySelector, args);

                joins.Add(
                    $"INNER JOIN \"{m._joinInner.Table.TableName}\" ON {l.CommandText} = {r.CommandText}"
                );

            } while ((m = m._upperQuery) != null);

            return joins.ToArray();
        }

        private string BuildLimitAndOffset(IList<SQLiteParameter> args)
        {
            string r = null;
            if (_limit.HasValue)
                r = " LIMIT " + _limit.Value;

            if (_offset.HasValue)
            {
                if (!_limit.HasValue)
                    r = " LIMIT -1 ";

                r += " OFFSET " + _offset.Value;
            }

            return r;
        }

        private string BuildOrderBy(IList<SQLiteParameter> args)
        {
            string t = null;

            if ((_orderBys != null) && (_orderBys.Count > 0))
                t = string.Join(",\n    ", _orderBys.Select(o => o.ColumnName + (o.Ascending ? "" : " DESC")).ToArray());

            return t;
        }

        private string BuildWhere(IList<SQLiteParameter> args)
        {
            CompileResult w = null;
            if (_where != null)
                w = CompileExpr(_where, args);

            return w?.CommandText;
        }
        #endregion

        protected object ConvertTo(object obj, Type t)
        {
            Type nut = Nullable.GetUnderlyingType(t);

            if (nut != null)
            {
                if (obj == null) return null;
                return Convert.ChangeType(obj, nut);
            }
            else
            {
                return Convert.ChangeType(obj, t);
            }
        }

        protected string GetSqlName(Expression expr)
        {
            var n = expr.NodeType;
            if (n == ExpressionType.GreaterThan)
                return ">";
            else if (n == ExpressionType.GreaterThanOrEqual)
            {
                return ">=";
            }
            else if (n == ExpressionType.LessThan)
            {
                return "<";
            }
            else if (n == ExpressionType.LessThanOrEqual)
            {
                return "<=";
            }
            else if (n == ExpressionType.And)
            {
                return "&";
            }
            else if (n == ExpressionType.AndAlso)
            {
                return "AND";
            }
            else if (n == ExpressionType.Or)
            {
                return "|";
            }
            else if (n == ExpressionType.OrElse)
            {
                return "OR";
            }
            else if (n == ExpressionType.Equal)
            {
                return "=";
            }
            else if (n == ExpressionType.NotEqual)
            {
                return "!=";
            }
            else
            {
                throw new NotSupportedException("Cannot get SQL for: " + n);
            }
        }

        protected void AddWhere(Expression pred)
        {
            if (_where == null)
            {
                _where = pred;
            }
            else
            {
                _where = Expression.AndAlso(_where, pred);
            }
        }


    }

    public class TableQuery<T> : TableQueryBase, IEnumerable<T>
    {

        TableQuery(SQLiteConnection conn, TableMapping table)
        {
            Connection = conn;
            Table = table;
        }

        public TableQuery(SQLiteConnection conn)
        {
            Connection = conn;
            Table = Connection.GetMapping(typeof(T));
        }

        public TableQuery<U> Clone<U>()
        {
            var q = new TableQuery<U>(Connection, Table);
            q._where = _where;
            q._deferred = _deferred;
            if (_orderBys != null)
            {
                q._orderBys = new List<Ordering>(_orderBys);
            }
            q._limit = _limit;
            q._offset = _offset;
            q._joinInner = _joinInner;
            q._joinInnerKeySelector = _joinInnerKeySelector;
            q._joinOuter = _joinOuter;
            q._joinOuterKeySelector = _joinOuterKeySelector;
            q._joinSelector = _joinSelector;
            q._selector = _selector;
            return q;
        }

        public TableQuery<T> Where(Expression<Func<T, bool>> predExpr)
        {
            if (predExpr.NodeType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)predExpr;
                var pred = lambda.Body;
                var q = Clone<T>();
                q.AddWhere(pred);
                return q;
            }
            else
            {
                throw new NotSupportedException("Must be a predicate");
            }
        }

        public TableQuery<T> Take(int n)
        {
            var q = Clone<T>();
            q._limit = n;
            return q;
        }

        public TableQuery<T> Skip(int n)
        {
            var q = Clone<T>();
            q._offset = n;
            return q;
        }

        public T ElementAt(int index)
        {
            return Skip(index).Take(1).First();
        }

        public TableQuery<T> Deferred()
        {
            var q = Clone<T>();
            q._deferred = true;
            return q;
        }
        public int Count()
        {
            return GenerateCommand("COUNT(*)").ExecuteScalar<int>();
        }
        public int Count(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).Count();
        }
        public TableQuery<T> OrderBy<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy<U>(orderExpr, true);
        }

        public TableQuery<T> OrderByDescending<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy<U>(orderExpr, false);
        }

        private TableQuery<T> AddOrderBy<U>(Expression<Func<T, U>> orderExpr, bool asc)
        {
            if (orderExpr.NodeType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)orderExpr;

                MemberExpression mem = null;

                var unary = lambda.Body as UnaryExpression;
                if (unary != null && unary.NodeType == ExpressionType.Convert)
                    mem = unary.Operand as MemberExpression;
                else
                    mem = lambda.Body as MemberExpression;

                if (mem != null && (mem.Expression.NodeType == ExpressionType.Parameter))
                    return RegisterOrderByClause(asc, Table.FindColumnWithPropertyName(mem.Member.Name));
                else
                {
                    getMappings(ref _mappings, this);
                    if (_mappings.ContainsKey(mem.Member.DeclaringType))
                    {
                        var column = _mappings[mem.Member.DeclaringType].Table.FindColumnWithPropertyName(mem.Member.Name);

                        if (column != null)
                            return RegisterOrderByClause(asc, column);
                    }

                    throw new NotSupportedException("Order By does not support: " + orderExpr);
                }
            }
            else
            {
                throw new NotSupportedException("Must be a predicate");
            }
        }

        private TableQuery<T> RegisterOrderByClause(bool asc, TableMapping.Column column)
        {
            var q = Clone<T>();

            if (q._orderBys == null)
                q._orderBys = new List<Ordering>();

            q._orderBys.Add(new Ordering
            {
                ColumnName = column.FullyQualifiedColumnName,
                Ascending = asc
            });

            return q;
        }

        public TableQuery<TResult> Join<TInner, TKey, TResult>(
            TableQuery<TInner> inner,
            Expression<Func<T, TKey>> outerKeySelector,
            Expression<Func<TInner, TKey>> innerKeySelector,
            Expression<Func<T, TInner, TResult>> resultSelector)
        {
            var q = new TableQuery<TResult>(Connection, Connection.GetMapping(typeof(TResult)))
            {
                _joinOuter = this,
                _joinOuterKeySelector = outerKeySelector,
                _joinInner = inner,
                _joinInnerKeySelector = innerKeySelector,
                _joinSelector = resultSelector,
            };
            _upperQuery = q;
            return q;
        }

        public TableQuery<TResult> Select<TResult>(Expression<Func<T, TResult>> selector)
        {
            var q = Clone<TResult>();
            q._selector = selector;
            return q;
        }

        public IEnumerator<T> GetEnumerator()
        {
            var command = GenerateCommand();

            if (_deferred)
                return command.ExecuteDeferredQuery<T>().GetEnumerator();

            return command.ExecuteQuery<T>().GetEnumerator();
        }

        public override string ToString()
        {
            var command = GenerateCommand();

            return command.CommandText;
        }


        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public T First()
        {
            var query = Take(1);
            return query.ToList<T>().First();
        }

        public T FirstOrDefault()
        {
            var query = Take(1);
            return query.ToList<T>().FirstOrDefault();
        }
    }

    public static class SQLite3
    {
        public enum Result : int
        {
            OK = 0,
            Error = 1,
            Internal = 2,
            Perm = 3,
            Abort = 4,
            Busy = 5,
            Locked = 6,
            NoMem = 7,
            ReadOnly = 8,
            Interrupt = 9,
            IOError = 10,
            Corrupt = 11,
            NotFound = 12,
            Full = 13,
            CannotOpen = 14,
            LockErr = 15,
            Empty = 16,
            SchemaChngd = 17,
            TooBig = 18,
            Constraint = 19,
            Mismatch = 20,
            Misuse = 21,
            NotImplementedLFS = 22,
            AccessDenied = 23,
            Format = 24,
            Range = 25,
            NonDBFile = 26,
            Row = 100,
            Done = 101
        }

        public enum ConfigOption : int
        {
            SingleThread = 1,
            MultiThread = 2,
            Serialized = 3
        }

#if !USE_CSHARP_SQLITE && !USE_WP8_NATIVE_SQLITE
        [DllImport("sqlite3", EntryPoint = "sqlite3_open", CallingConvention = CallingConvention.Cdecl)]
        public static extern Result Open([MarshalAs(UnmanagedType.LPStr)] string filename, out Sqlite3DatabaseHandle db);

        [DllImport("sqlite3", EntryPoint = "sqlite3_open_v2", CallingConvention = CallingConvention.Cdecl)]
        public static extern Result Open([MarshalAs(UnmanagedType.LPStr)] string filename, out Sqlite3DatabaseHandle db, int flags, Sqlite3DatabaseHandle zvfs);

        [DllImport("sqlite3", EntryPoint = "sqlite3_open_v2", CallingConvention = CallingConvention.Cdecl)]
        public static extern Result Open(byte[] filename, out Sqlite3DatabaseHandle db, int flags, Sqlite3DatabaseHandle zvfs);

        [DllImport("sqlite3", EntryPoint = "sqlite3_open16", CallingConvention = CallingConvention.Cdecl)]
        public static extern Result Open16([MarshalAs(UnmanagedType.LPWStr)] string filename, out Sqlite3DatabaseHandle db);

        [DllImport("sqlite3", EntryPoint = "sqlite3_enable_load_extension", CallingConvention = CallingConvention.Cdecl)]
        public static extern Result EnableLoadExtension(Sqlite3DatabaseHandle db, int onoff);

        [DllImport("sqlite3", EntryPoint = "sqlite3_close", CallingConvention = CallingConvention.Cdecl)]
        public static extern Result Close(Sqlite3DatabaseHandle db);

        [DllImport("sqlite3", EntryPoint = "sqlite3_config", CallingConvention = CallingConvention.Cdecl)]
        public static extern Result Config(ConfigOption option);

        [DllImport("sqlite3", EntryPoint = "sqlite3_win32_set_directory", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Unicode)]
        public static extern int SetDirectory(uint directoryType, string directoryPath);

        [DllImport("sqlite3", EntryPoint = "sqlite3_busy_timeout", CallingConvention = CallingConvention.Cdecl)]
        public static extern Result BusyTimeout(Sqlite3DatabaseHandle db, int milliseconds);

        [DllImport("sqlite3", EntryPoint = "sqlite3_changes", CallingConvention = CallingConvention.Cdecl)]
        public static extern int Changes(Sqlite3DatabaseHandle db);

        [DllImport("sqlite3", EntryPoint = "sqlite3_prepare_v2", CallingConvention = CallingConvention.Cdecl)]
        public static extern Result Prepare2(Sqlite3DatabaseHandle db, [MarshalAs(UnmanagedType.LPStr)] string sql, int numBytes, out Sqlite3DatabaseHandle stmt, Sqlite3DatabaseHandle pzTail);

        public static Sqlite3DatabaseHandle Prepare2(Sqlite3DatabaseHandle db, string query)
        {
            Sqlite3DatabaseHandle stmt;
            var r = Prepare2(db, query, query.Length, out stmt, Sqlite3DatabaseHandle.Zero);
            if (r != Result.OK)
            {
                throw SQLiteException.New(r, GetErrmsg(db));
            }
            return stmt;
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_step", CallingConvention = CallingConvention.Cdecl)]
        public static extern Result Step(Sqlite3DatabaseHandle stmt);

        [DllImport("sqlite3", EntryPoint = "sqlite3_reset", CallingConvention = CallingConvention.Cdecl)]
        public static extern Result Reset(Sqlite3DatabaseHandle stmt);

        [DllImport("sqlite3", EntryPoint = "sqlite3_finalize", CallingConvention = CallingConvention.Cdecl)]
        public static extern Result Finalize(Sqlite3DatabaseHandle stmt);

        [DllImport("sqlite3", EntryPoint = "sqlite3_last_insert_rowid", CallingConvention = CallingConvention.Cdecl)]
        public static extern long LastInsertRowid(Sqlite3DatabaseHandle db);

        [DllImport("sqlite3", EntryPoint = "sqlite3_errmsg16", CallingConvention = CallingConvention.Cdecl)]
        public static extern Sqlite3DatabaseHandle Errmsg(Sqlite3DatabaseHandle db);

        public static string GetErrmsg(Sqlite3DatabaseHandle db)
        {
            return Marshal.PtrToStringUni(Errmsg(db));
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_bind_parameter_index", CallingConvention = CallingConvention.Cdecl)]
        public static extern int BindParameterIndex(Sqlite3DatabaseHandle stmt, [MarshalAs(UnmanagedType.LPStr)] string name);

        [DllImport("sqlite3", EntryPoint = "sqlite3_bind_null", CallingConvention = CallingConvention.Cdecl)]
        public static extern int BindNull(Sqlite3DatabaseHandle stmt, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_bind_int", CallingConvention = CallingConvention.Cdecl)]
        public static extern int BindInt(Sqlite3DatabaseHandle stmt, int index, int val);

        [DllImport("sqlite3", EntryPoint = "sqlite3_bind_int64", CallingConvention = CallingConvention.Cdecl)]
        public static extern int BindInt64(Sqlite3DatabaseHandle stmt, int index, long val);

        [DllImport("sqlite3", EntryPoint = "sqlite3_bind_double", CallingConvention = CallingConvention.Cdecl)]
        public static extern int BindDouble(Sqlite3DatabaseHandle stmt, int index, double val);

        [DllImport("sqlite3", EntryPoint = "sqlite3_bind_text16", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Unicode)]
        public static extern int BindText(Sqlite3DatabaseHandle stmt, int index, [MarshalAs(UnmanagedType.LPWStr)] string val, int n, Sqlite3DatabaseHandle free);

        [DllImport("sqlite3", EntryPoint = "sqlite3_bind_blob", CallingConvention = CallingConvention.Cdecl)]
        public static extern int BindBlob(Sqlite3DatabaseHandle stmt, int index, byte[] val, int n, Sqlite3DatabaseHandle free);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_count", CallingConvention = CallingConvention.Cdecl)]
        public static extern int ColumnCount(Sqlite3DatabaseHandle stmt);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_name", CallingConvention = CallingConvention.Cdecl)]
        public static extern Sqlite3DatabaseHandle ColumnName(Sqlite3DatabaseHandle stmt, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_name16", CallingConvention = CallingConvention.Cdecl)]
        static extern Sqlite3DatabaseHandle ColumnName16Internal(Sqlite3DatabaseHandle stmt, int index);
        public static string ColumnName16(Sqlite3DatabaseHandle stmt, int index)
        {
            return Marshal.PtrToStringUni(ColumnName16Internal(stmt, index));
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_type", CallingConvention = CallingConvention.Cdecl)]
        public static extern ColType ColumnType(Sqlite3DatabaseHandle stmt, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_int", CallingConvention = CallingConvention.Cdecl)]
        public static extern int ColumnInt(Sqlite3DatabaseHandle stmt, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_int64", CallingConvention = CallingConvention.Cdecl)]
        public static extern long ColumnInt64(Sqlite3DatabaseHandle stmt, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_double", CallingConvention = CallingConvention.Cdecl)]
        public static extern double ColumnDouble(Sqlite3DatabaseHandle stmt, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_text", CallingConvention = CallingConvention.Cdecl)]
        public static extern Sqlite3DatabaseHandle ColumnText(Sqlite3DatabaseHandle stmt, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_text16", CallingConvention = CallingConvention.Cdecl)]
        public static extern Sqlite3DatabaseHandle ColumnText16(Sqlite3DatabaseHandle stmt, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_blob", CallingConvention = CallingConvention.Cdecl)]
        public static extern Sqlite3DatabaseHandle ColumnBlob(Sqlite3DatabaseHandle stmt, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_bytes", CallingConvention = CallingConvention.Cdecl)]
        public static extern int ColumnBytes(Sqlite3DatabaseHandle stmt, int index);

        public static string ColumnString(Sqlite3DatabaseHandle stmt, int index)
        {
            return Marshal.PtrToStringUni(SQLite3.ColumnText16(stmt, index));
        }

        public static byte[] ColumnByteArray(Sqlite3DatabaseHandle stmt, int index)
        {
            int length = ColumnBytes(stmt, index);
            var result = new byte[length];
            if (length > 0)
                Marshal.Copy(ColumnBlob(stmt, index), result, 0, length);
            return result;
        }
#else
        public static Result Open(string filename, out Sqlite3DatabaseHandle db)
        {
            return (Result) Sqlite3.sqlite3_open(filename, out db);
        }

        public static Result Open(string filename, out Sqlite3DatabaseHandle db, int flags, IntPtr zVfs)
        {
#if USE_WP8_NATIVE_SQLITE
            return (Result)Sqlite3.sqlite3_open_v2(filename, out db, flags, "");
#else
            return (Result)Sqlite3.sqlite3_open_v2(filename, out db, flags, null);
#endif
        }

        public static Result Close(Sqlite3DatabaseHandle db)
        {
            return (Result)Sqlite3.sqlite3_close(db);
        }

        public static Result BusyTimeout(Sqlite3DatabaseHandle db, int milliseconds)
        {
            return (Result)Sqlite3.sqlite3_busy_timeout(db, milliseconds);
        }

        public static int Changes(Sqlite3DatabaseHandle db)
        {
            return Sqlite3.sqlite3_changes(db);
        }

        public static Sqlite3Statement Prepare2(Sqlite3DatabaseHandle db, string query)
        {
            Sqlite3Statement stmt = default(Sqlite3Statement);
#if USE_WP8_NATIVE_SQLITE
            var r = Sqlite3.sqlite3_prepare_v2(db, query, out stmt);
#else
            stmt = new Sqlite3Statement();
            var r = Sqlite3.sqlite3_prepare_v2(db, query, -1, ref stmt, 0);
#endif
            if (r != 0)
            {
                throw SQLiteException.New((Result)r, GetErrmsg(db));
            }
            return stmt;
        }

        public static Result Step(Sqlite3Statement stmt)
        {
            return (Result)Sqlite3.sqlite3_step(stmt);
        }

        public static Result Reset(Sqlite3Statement stmt)
        {
            return (Result)Sqlite3.sqlite3_reset(stmt);
        }

        public static Result Finalize(Sqlite3Statement stmt)
        {
            return (Result)Sqlite3.sqlite3_finalize(stmt);
        }

        public static long LastInsertRowid(Sqlite3DatabaseHandle db)
        {
            return Sqlite3.sqlite3_last_insert_rowid(db);
        }

        public static string GetErrmsg(Sqlite3DatabaseHandle db)
        {
            return Sqlite3.sqlite3_errmsg(db);
        }

        public static int BindParameterIndex(Sqlite3Statement stmt, string name)
        {
            return Sqlite3.sqlite3_bind_parameter_index(stmt, name);
        }

        public static int BindNull(Sqlite3Statement stmt, int index)
        {
            return Sqlite3.sqlite3_bind_null(stmt, index);
        }

        public static int BindInt(Sqlite3Statement stmt, int index, int val)
        {
            return Sqlite3.sqlite3_bind_int(stmt, index, val);
        }

        public static int BindInt64(Sqlite3Statement stmt, int index, long val)
        {
            return Sqlite3.sqlite3_bind_int64(stmt, index, val);
        }

        public static int BindDouble(Sqlite3Statement stmt, int index, double val)
        {
            return Sqlite3.sqlite3_bind_double(stmt, index, val);
        }

        public static int BindText(Sqlite3Statement stmt, int index, string val, int n, IntPtr free)
        {
#if USE_WP8_NATIVE_SQLITE
            return Sqlite3.sqlite3_bind_text(stmt, index, val, n);
#else
            return Sqlite3.sqlite3_bind_text(stmt, index, val, n, null);
#endif
        }

        public static int BindBlob(Sqlite3Statement stmt, int index, byte[] val, int n, IntPtr free)
        {
#if USE_WP8_NATIVE_SQLITE
            return Sqlite3.sqlite3_bind_blob(stmt, index, val, n);
#else
            return Sqlite3.sqlite3_bind_blob(stmt, index, val, n, null);
#endif
        }

        public static int ColumnCount(Sqlite3Statement stmt)
        {
            return Sqlite3.sqlite3_column_count(stmt);
        }

        public static string ColumnName(Sqlite3Statement stmt, int index)
        {
            return Sqlite3.sqlite3_column_name(stmt, index);
        }

        public static string ColumnName16(Sqlite3Statement stmt, int index)
        {
            return Sqlite3.sqlite3_column_name(stmt, index);
        }

        public static ColType ColumnType(Sqlite3Statement stmt, int index)
        {
            return (ColType)Sqlite3.sqlite3_column_type(stmt, index);
        }

        public static int ColumnInt(Sqlite3Statement stmt, int index)
        {
            return Sqlite3.sqlite3_column_int(stmt, index);
        }

        public static long ColumnInt64(Sqlite3Statement stmt, int index)
        {
            return Sqlite3.sqlite3_column_int64(stmt, index);
        }

        public static double ColumnDouble(Sqlite3Statement stmt, int index)
        {
            return Sqlite3.sqlite3_column_double(stmt, index);
        }

        public static string ColumnText(Sqlite3Statement stmt, int index)
        {
            return Sqlite3.sqlite3_column_text(stmt, index);
        }

        public static string ColumnText16(Sqlite3Statement stmt, int index)
        {
            return Sqlite3.sqlite3_column_text(stmt, index);
        }

        public static byte[] ColumnBlob(Sqlite3Statement stmt, int index)
        {
            return Sqlite3.sqlite3_column_blob(stmt, index);
        }

        public static int ColumnBytes(Sqlite3Statement stmt, int index)
        {
            return Sqlite3.sqlite3_column_bytes(stmt, index);
        }

        public static string ColumnString(Sqlite3Statement stmt, int index)
        {
            return Sqlite3.sqlite3_column_text(stmt, index);
        }

        public static byte[] ColumnByteArray(Sqlite3Statement stmt, int index)
        {
            return ColumnBlob(stmt, index);
        }
#endif

        public enum ColType : int
        {
            Integer = 1,
            Float = 2,
            Text = 3,
            Blob = 4,
            Null = 5
        }
    }
}
