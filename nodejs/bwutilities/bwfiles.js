const fs = require('fs');
var _ = require('lodash');
var bt = require('bytes');
var path = require('path');

var config = require('./config.json');

fs.readdirAsync = function (dir) {
  return new Promise(function (resolve, reject) {
    fs.readdir(dir, function (err, list) {
      if (err) {
        reject(err);
      } else {
        resolve(list);
      }
    });
  });
}

fs.statAsync = function (file) {
  return new Promise(function (resolve, reject) {
    fs.stat(file, function (err, stat) {
      if (err) {
        reject(err);
      } else {
        resolve(stat);
      }
    });
  });
}

var arquivoGrande = function (caminho, tamanho) {
  this.caminho = caminho;
  this.tamanho = tamanho;
}

function verificaDiretorio(dir, tamanho) {
  return fs.readdirAsync(dir).then(function (list) {
    return Promise.all(list.map(function (file) {
      file = path.join(dir, file);
      return fs.statAsync(file).then(function (stat) {
        if (stat.isDirectory()) {
          return verificaDiretorio(file, tamanho);
        } else {
          if ((stat.size >= bt.parse(tamanho)) && (config.ignoreExtensions.indexOf(path.extname(file)) < 0)) {
            return new arquivoGrande(file, bt.format(stat.size));
          } else {
            return null;
          }
        }
      });
    }));
  }).then(function (results) {
    return Array.prototype.concat.apply([], results);
  });
}

module.exports = {
  verificaDiretorio: verificaDiretorio
};