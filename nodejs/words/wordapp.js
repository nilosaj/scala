var oracledb = require('oracledb');
var dbConfig = require('./dbconfig.js');
var persistence = require('./persistence.js');


function init() {
  
  oracledb.createPool(
    {
      user: dbConfig.user,
      password: dbConfig.password,
      connectString: dbConfig.connectString,
       poolMax: 20, // maximum size of the pool. Increase UV_THREADPOOL_SIZE if you increase poolMax
       poolMin: 1 // start with no connections; let the pool shrink completely
      // poolIncrement: 1, // only grow the pool by one connection at a time
      // poolTimeout: 60, // terminate connections that are idle in the pool for 60 seconds
      // poolPingInterval: 60, // check aliveness of connection if in the pool for 60 seconds
      // queueRequests: true, // let Node.js queue new getConnection() requests if all pool connections are in use
      // queueTimeout: 60000, // terminate getConnection() calls in the queue longer than 60000 milliseconds
      // poolAlias: 'myalias' // could set an alias to allow access to the pool via a name
      // stmtCacheSize: 30 // number of statements that are cached in the statement cache of each connection
    },
    function(err, pool) {
      if (err) {
        console.error("createPool() error: " + err.message);
        return;
      }
     
     for (var i=0;i< 10000;i++){
            //console.log("pool criado ",pool);
            testeConsulta(geraNumeroAleatorio(1,800),pool,(retorno)=>{
                console.log(retorno)
            })
     } 
     

    }
  );
}

function testeConsulta(id,pool,callback){
// Checkout a connection from the pool
  pool.getConnection(function(err, connection) {
    if (err) {
      console.log('Deu Erro 1');
      return;
    }

     console.log("Connections open: " + pool.connectionsOpen);
     console.log("Connections in use: " + pool.connectionsInUse);

    connection.execute(
      " SELECT WORDSAMPLE FROM TB_WORDS WHERE  ID_WORD = :id",
      [id], // bind variable value
      function(err, result) {
        if (err) {
          connection.close(function(err) {
            if (err) {
              // Just logging because handleError call below will have already
              // ended the response.
              console.error("execute() error release() error", err);
            }
          });
          console.log('Deu Erro 2');
          return;
        }

        
        /* Release the connection back to the connection pool */
        connection.close(function(err) {
          if (err) {
            console.log('Deu Erro 2');
          } else {
            //console.log('Acabou ',result['rows']);
            callback(result['rows']);
          }
        });
      }
    );
  });

}


function geraNumeroAleatorio(minimo , maximo){
    return Math.floor(Math.random()*(maximo-minimo+1)+minimo);
}

init()