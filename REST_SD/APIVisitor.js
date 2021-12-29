const express = require("express");
const appSD = express;

// Se define el puerto
const puerto = 3001;

const mysql = require("mysql");
const bodyparser = require("body-parser");
const { response } = require("express");

// Conexion con la Base de Datos
const connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '1234',
    database: 'fwq_bbdd'
});

// Comprobar conexion con base de datos
connection.connect(error=> {
    if (error) throw error;
    console.log('Conexion establecida');
});

// Listar usuarios
appSD.get("/usuarios",(request, reponse) => {
    console.log('Obtener los usuarios');

    const sql = 'SELECT * FROM fwq_bbdd.usuarios';
    connection.query(sql,(error,resultado)=>{
        if (error) throw error;
        if (resultado.length > 0) {
            response.json(resultado);
        } else {
            response.send('No hay usuarios para listar');
        }
    });
});

// Mostrar un usuario
appSD.get("/usuarios/:id",(request, reponse) => {
    console.log('Obtener los usuarios');

    const {id} = request.params;
    const sql = 'SELECT * FROM fwq_bbdd.usuarios WHERE ID = ' + id;
    connection.query(sql,(error,resultado)=>{
        if (error) throw error;
        if (resultado.length > 0) {
            response.json(resultado);
        } else {
            response.send('El usuario especificado no existe');
        }
    });
});

// Registrar usuario
appSD.post("/usuarios",(request,response) => {
    console.log('Se ve a añadir un usuario');
    const sql = 'INSERT INTO fwq_bbdd.usuarios SET ?';

    const usuarioObj = {
        Alias = request.body.alias,
        Nombre = request.body.nombre,
        Contrasenya = request.body.contrasenya
    }
    connection.query(sql,usuarioObj, error => {
        if (error) throw error;
        response.send('Usuario insertado');
    });
});

// Modificar un usuario
appSD.put("/usuarios/:id",(request, response) => {
    console.log('Se va a modificar un usuario');

    const {id} = request.params;
    const {nombre,contrasenya} = request.body;
    const sql = 'UPDATE fwq_bbdd.usuarios SET nombre=\'' + nombre + 
        '\', contrasenya=\'' + contrasenya + '\' WHERE Alias=\'' + id + '\'';
    connection.query(sql,error => {
        if (error) throw error;
        response.send('Usuario modificado');
    });
});

// Borrar usuario
appSD.delete("/usuarios/:id",(request, response) => {
    console.log('Se va a borrar un usuario');

    const {id} = request.params;
    const sql = 'DELETE FROM fwq_bbdd.usuarios WHERE Alias = ' + id;
    connection.query(sql,error => {
        if (error) throw error;
        response.send('Usuario borrado');
    });
});