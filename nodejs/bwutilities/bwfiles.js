const fs = require('fs');
var _ = require('lodash');
var bt = require('bytes');
var path = require('path');

var config = require('./config.json');

var arquivosGrandes = []
var arquivoGrande = function(caminho,tamanho){
   this.caminho = caminho;
   this.tamanho = tamanho;
}

exports.verificaDiretorio = function verificaDiretorio(diretorio,tamanho,callback){
    fs.readdir(diretorio,(err,arquivos)=>{
        arquivos.forEach(arquivo => {
         var caminho = path.join(diretorio,arquivo);
         var status = fs.statSync(caminho);
         if (status.isDirectory()){
           verificaDiretorio(caminho,tamanho)
         }else{
           if ((status.size >= bt.parse(tamanho)) && ( config.ignoreExtensions.indexOf(path.extname(arquivo))<0)){  
                console.log(caminho,'  [',bt.format(status.size),']')
                var arqGrande = new arquivoGrande(caminho,bt.format(status.size))  
                arquivosGrandes.push(arqGrande)   
           }
        }
      });
    return callback(arquivosGrandes)  
  });
    
};


