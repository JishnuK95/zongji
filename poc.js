// Client code
const ZongJi = require("./");
const mysql = require("mysql");
const nodeSqlParser = require("node-sql-parser");
const parser = new nodeSqlParser.Parser();

let zongji;
let mysqlConnection;

const parseQuery = (query) => {
   // Convert SQL query to AST (Abstract Syntax Tree)
   const ast = parser.astify(query);
   const parsedData = nodeSqlParser.parse(query);
   let action = "";
   let table = "";

   switch (ast.type) {
      case "insert":
         action = "INSERT";

         try {
            console.log("parsedData-insert: ", parsedData);
         } catch (error) {}

         try {
            table = ast?.table[0]?.table ?? "Unknown";
         } catch (error) {
            console.error("Error while parsing INSERT: ", error?.message);

            table = "Unknown";
         }
         break;
      case "update":
         action = "UPDATE";

         try {
            console.log("parsedData-update: ", parsedData);
         } catch (error) {}

         try {
            table = ast?.table[0]?.table ?? "Unknown";
         } catch (error) {
            console.error("Error while parsing UPDATE: ", error?.message);

            table = "Unknown";
         }
         break;
      case "delete":
         action = "DELETE";

         try {
            console.log("parsedData-delete: ", parsedData);
         } catch (error) {}

         try {
            table = ast?.table[0]?.table ?? "Unknown";
         } catch (error) {
            console.error("Error while parsing DELETE: ", error?.message);

            table = "Unknown";
         }
         break;
      default:
         action = ast?.type ?? "-";
         table = "Unknown";
         break;
   }

   return { action, table };
};

const startZongJi = () => {
   zongji = new ZongJi({
      host: "fleet-staging-load-tested.cqpvk3xrt3yu.us-west-1.rds.amazonaws.com",
      user: "admin",
      password: "uq7wmda8U9ReEZR",
      // debug: true
   });

   // Specify the events you want to listen to
   zongji.on("binlog", function (evt) {
      if (evt.getTypeName() === "Query") {
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
   zongji.on("error", function (error) {
      console.error("ZongJi error:", error);

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
