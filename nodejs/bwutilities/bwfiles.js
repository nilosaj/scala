const fs = require('fs');
var _ = require('lodash');
var bt = require('bytes');
var path = require('path');



exports.verificaDiretorio = function verificaDiretorio(diretorio,tamanho){
    fs.readdir(diretorio,(err,arquivos)=>{
        console.log('diretorio :',diretorio);
        arquivos.forEach(arquivo => {
         var caminho = path.join(diretorio,arquivo);
         var status = fs.statSync(caminho);
         if (status.isDirectory()){
           verificaDiretorio(caminho)
         }else{
           if (status.size >= bt.parse(tamanho)){  
                console.log(caminho,'  [',bt.format(status.size),']')
           }
         }
        });
    });

};
