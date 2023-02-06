const express = require("express");
const appSD = express();

// Se define el puerto 
const port=3000;

const mysql = require ("mysql");
const bodyParser = require("body-parser");

// Conexion con la Base de Datos
const connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '1234',
    database: 'fwq_bbdd'
});

appSD.get("/",(reg, res) => {
    res.json({message:'Página de inicio de la aplicación'})
});

// Ejecutar aplicacion
appSD.listen(port, ()=> {
    console.log('Ejecutando la aplicacion en el puerto ' + port);
});

// Comprobar conexion con base de datos
connection.connect(error=> {
    if (error) throw error;
    console.log('Conexion establecida');
});

// Listar los usuarios
appSD.get("/usuarios",(request, response) => {
    console.log('Listado de los usuarios');

    const sql = 'SELECT * FROM fwq_bbdd.usuarios';
    connection.query(sql, (error,resultado)=>{
        if (error) throw error;
        if (resultado.length > 0) {
            response.json(resultado);
        } else {
            response.send('No hay usuarios')
        }
    });
});

// Mostrar un usuario especifico
appSD.get("/usuarios/:id",(request,response) => {
    console.log('Usuario especifico');

    const {id} = request.params;
    const sql = 'SELECT * FROM fwq_bbdd.usuarios WHERE Alias = \'' + id + '\'';
    connection.query(sql,(error,resultado)=>{
        if (error) throw error;
        if (resultado.length > 0) {
            response.json(resultado);
        } else {
            response.send('No existe el usuario especificado');
        }
    });
});

// Listar las atracciones
appSD.get("/atracciones",(request, response) => {
    console.log('Listado de las atracciones');

    const sql = 'SELECT * FROM fwq_bbdd.atracciones';
    connection.query(sql, (error,resultado)=>{
        if (error) throw error;
        if (resultado.length > 0) {
            response.json(resultado);
        } else {
            response.send('No hay atracciones')
        }
    });
});

// Mostrar un usuario especifico
appSD.get("/atracciones/:id",(request,response) => {
    console.log('atraccion especifica');

    const {id} = request.params;
    const sql = 'SELECT * FROM fwq_bbdd.atracciones WHERE ID = \'' + id + '\'';
    connection.query(sql,(error,resultado)=>{
        if (error) throw error;
        if (resultado.length > 0) {
            response.json(resultado);
        } else {
            response.send('No existe la atraccion especificada');
        }
    });
});

// Mostrar el mapa
appSD.get("/mapa",(request, response) => {
    console.log('Obtener el mapa del parque');

    const sql = 'SELECT * FROM fwq_bbdd.cadenamapa WHERE ID = 1';
    connection.query(sql,(error,resultado)=>{
        if (error) throw error;
        if (resultado.length > 0) {
            response.json(resultado);
        } else {
            response.send('No hay un mapa formado');
        }
    });
});