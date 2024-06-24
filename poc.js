// Client code
const ZongJi = require("./");
const mysql = require("mysql");
const nodeSqlParser = require("node-sql-parser");
const parser = new nodeSqlParser.Parser();

let zongji;
let mysqlConnection;

const parseInsertData = (ast) => {
   try {
      let insertObject = {};

      console.log("ast-insert-columns: ", ast?.columns);
      console.log("ast-insert-values: ", ast?.values[0]?.value);

      insertObject.columns = ast?.columns ?? [];
      insertObject.values = ast?.values[0]?.value ?? [];

      insertObject.objectData = {};

      if (insertObject.columns && insertObject.columns?.length > 0) {
         for (const [index, element] of insertObject.columns.entries()) {
            insertObject.objectData[element] = insertObject.values[index]?.value;
         }
      }

      return insertObject;
   } catch (error) {
      throw error;
   }
};

const parseUpdateData = (ast) => {
   try {
      let updateObject = {};

      console.log("ast-update-set: ", ast?.set);
      console.log("ast-update-where: ", ast?.where);

      updateObject.set = ast?.set[0]?.value ?? [];
      updateObject.where = ast?.where ?? {};
      updateObject.objectData = {};
      updateObject.whereClaue = {};

      for (const { column, value } of updateObject.set) {
         updateObject.objectData[column] = value;
      }

      return updateObject;
   } catch (error) {
      throw error;
   }
};

const parseDeleteData = (ast) => {
   try {
      let deleteObject = {};

      console.log("ast-delete-where: ", ast?.where);

      deleteObject.where = ast?.where ?? {};
      deleteObject.whereClaue = {};

      return deleteObject;
   } catch (error) {
      throw error;
   }
};

const parseQuery = (query) => {
   let action = "";
   let table = "";
   let insertObject = {};
   let updateObject = {};
   let deleteObject = {};

   // Convert SQL query to AST (Abstract Syntax Tree)
   let ast;

   try {
      ast = parser.astify(query);
   } catch (error) {
      console.error(`\nParsing failed in parseQuery(${query}): ` + error?.message + "\n");

      return { action, table };
   }

   switch (ast.type) {
      case "insert":
         action = "INSERT";

         try {
            table = ast?.table[0]?.table ?? "";
         } catch (error) {
            console.error("Error while parsing INSERT: " + error?.message);
         }

         try {
            insertObject = parseInsertData(ast);
         } catch (error) {
            console.error("Error insert parsing data: " + error.message);

            insertObject.columns = [];
            insertObject.values = [];
            insertObject.objectData = {};
         }

         break;
      case "update":
         action = "UPDATE";

         try {
            table = ast?.table[0]?.table ?? "";
         } catch (error) {
            console.error("Error while parsing UPDATE: " + error?.message);
         }

         try {
            updateObject = parseUpdateData(ast);
         } catch (error) {
            console.error("Error update parsing data: " + error.message);

            updateObject.where = {};
            updateObject.whereClaue = {};
            updateObject.objectData = {};
            updateObject.whereClaue = {};
         }

         break;
      case "delete":
         action = "DELETE";

         try {
            table = ast?.table[0]?.table ?? "";
         } catch (error) {
            console.error("Error while parsing DELETE: " + error?.message);
         }

         try {
            deleteObject = parseDeleteData(ast);
         } catch (error) {
            console.error("Error delete parsing data: " + error.message);

            deleteObject.where = {};
            deleteObject.whereClaue = {};
         }

         break;
      default:
         break;
   }

   return { action, table, insertObject, updateObject, deleteObject };
};

const startZongJi = () => {
   zongji = new ZongJi({
      host: "fleet-staging-load-tested.cqpvk3xrt3yu.us-west-1.rds.amazonaws.com",
      user: "admin",
      password: "uq7wmda8U9ReEZR",
      // debug: true
   });

   // Specify the events you want to listen to
   zongji.on("binlog", (evt) => {
      if (evt.getTypeName() === "Query") {
         console.log("\nThe complete query: " + evt?.query, "\n");

         const evtQuery = evt.query.trim();

         if (evtQuery !== "BEGIN" && !evtQuery?.toLowerCase().includes("mysql.")) {
            let parsedQuery = parseQuery(evtQuery);

            console.log(parsedQuery);

            let insertLogQuery = `INSERT INTO log_queries (action_type, table_name, query_string) VALUES (?, ?, ?);`;

            mysqlConnection.query(
               insertLogQuery,
               [parsedQuery?.action, parsedQuery?.table, evtQuery],
               (error, results) => {
                  if (error) {
                     throw error;
                  }

                  console.log("The solution is: ", results);
               }
            );
         }
      }
   });

   // Handle disconnections and errors
   zongji.on("error", (error) => {
      console.error("ZongJi error:" + error?.message);

      if (error.code === "PROTOCOL_CONNECTION_LOST" || error.fatal) {
         console.log("Reconnecting ZongJi...");

         zongji.stop(() => {
            startZongJi(); // Restart ZongJi
         });
      }
   });

   // Start listening for events
   zongji.start({
      // Start at the end of the binlog
      startAtEnd: true,
      includeEvents: ["query"],
   });

   console.log("ZongJi started");
};

const createConnection = () => {
   mysqlConnection = mysql.createConnection({
      host: "192.168.0.8",
      user: "mysqlsauser",
      password: "software",
      database: "fleet_incremental_data_queries_test",
   });

   mysqlConnection.connect((err) => {
      if (err) {
         console.error("Error connecting to MySQL:", err);
         setTimeout(createConnection, 2000); // Reattempt mysqlConnection after 2 seconds
      } else {
         console.log("Connected to MySQL");
         startZongJi(); // Start ZongJi once connected
      }
   });

   mysqlConnection.on("error", (err) => {
      console.error("MySQL error:", err);

      if (err.code === "PROTOCOL_CONNECTION_LOST" || err.fatal) {
         console.log("Reconnecting MySQL...");
         mysqlConnection.destroy();

         createConnection(); // Reattempt mysqlConnection
      }
   });
};

// Start the process
createConnection();

// Handle graceful shutdown
process.on("SIGINT", () => {
   console.log("Stopping ZongJi and MySQL connection...");

   zongji.stop();
   mysqlConnection.end();
   process.exit();
});
