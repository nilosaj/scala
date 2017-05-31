var persistence = require('./persistence.js');
var redis = require("redis");
var yargs = require("yargs");


var inputargs = yargs.argv;

var datainicio = getDateTime()

if (inputargs.idword != null){
      persistence.retornaPool((pool) => {
          client = redis.createClient(32768, "localhost");
          client.on("error", function (err) {
              console.log("Error " + err);
          });
          console.log("######## Buscando por "+inputargs.idword+"########") 
          persistence.consultaRedis(inputargs.idword, client, 0, pool, (retornoRedis) => {
                
                    client.quit();
                    console.log("Inicio :"+datainicio+"   Fim :"+getDateTime())
                

          })
        
      })


}else{
  persistence.retornaPool((pool) => {
    client = redis.createClient(32768, "localhost");

    client.on("error", function (err) {
      console.log("Error " + err);
    });
    console.log("######## Busca em Massa ########")
    
    var execucoes = 0
    if (inputargs.execucoes != null){
      execucoes = inputargs.execucoes
    }else{
      execucoes= 10
    }
    
    let proms = []
    for (let i = 0; i < execucoes; i++) {

      setTimeout(()=>{persistence.consultaRedis(geraNumeroAleatorio(1, 1699), client, i, pool, (retornoRedis) => {

        proms.push(retornoRedis)
        if (proms.length == execucoes) {
          client.quit();
          console.log("Inicio :"+datainicio+"   Fim :"+getDateTime())
        }

      })},geraNumeroAleatorio(10,50))

    }


  })

}





function geraNumeroAleatorio(minimo, maximo) {
  return Math.floor(Math.random() * (maximo - minimo + 1) + minimo);
}


function getDateTime() {

    var date = new Date();

    var hour = date.getHours();
    hour = (hour < 10 ? "0" : "") + hour;

    var min  = date.getMinutes();
    min = (min < 10 ? "0" : "") + min;

    var sec  = date.getSeconds();
    sec = (sec < 10 ? "0" : "") + sec;

    var year = date.getFullYear();

    var month = date.getMonth() + 1;
    month = (month < 10 ? "0" : "") + month;

    var day  = date.getDate();
    day = (day < 10 ? "0" : "") + day;

    return year + ":" + month + ":" + day + ":" + hour + ":" + min + ":" + sec;

}

