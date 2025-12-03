const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(cors());

// Conexión a la Base de Datos
mongoose.connect(process.env.MONGO_URI)
    .then(() => console.log("Conectado a MongoDB Atlas"))
    .catch(err => console.error("Error conectando:", err));

// Modelo de datos (para leer la colección trips)
const tripSchema = new mongoose.Schema({}, { strict: false });
const Trip = mongoose.model('Trip', tripSchema, 'trips');

// Ruta de prueba
app.get('/', (req, res) => {
    res.send('API del Examen BigData funcionando. Ve a /dashboard.html');
});

// 1. Distribución por tipo de usuario
app.get('/api/users', async (req, res) => {
    try {
        const data = await Trip.aggregate([
            { $group: { _id: "$usertype", total: { $sum: 1 }, avgDuration: { $avg: "$tripduration" } } }
        ]);
        res.json(data);
    } catch (error) { res.status(500).json({ error: error.message }); }
});

// 2. Viajes por hora (Con filtro opcional ?hora=14)
app.get('/api/hours', async (req, res) => {
    const { hora } = req.query;
    let pipeline = [{ $project: { h: { $hour: "$start time" }, d: "$tripduration" } }];
    
    if (hora) pipeline.push({ $match: { h: parseInt(hora) } }); // Filtro solicitado
    
    pipeline.push(
        { $group: { _id: "$h", total: { $sum: 1 }, avgDuration: { $avg: "$d" } } },
        { $sort: { _id: 1 } }
    );
    const data = await Trip.aggregate(pipeline);
    res.json(data);
});

// 3. Viajes por día
app.get('/api/days', async (req, res) => {
    const data = await Trip.aggregate([
        { $group: { _id: { $dateToString: { format: "%Y-%m-%d", date: "$start time" } }, total: { $sum: 1 } } },
        { $sort: { _id: 1 } }
    ]);
    res.json(data);
});

// 4. Estaciones populares (Con limite variable ?limit=5)
app.get('/api/stations', async (req, res) => {
    const limite = parseInt(req.query.limit) || 10; // Por defecto 10
    const data = await Trip.aggregate([
        { $group: { _id: "$start station id", name: { $first: "$start station name" }, avgDuration: { $avg: "$tripduration" }, total: { $sum: 1 } } },
        { $sort: { total: -1 } },
        { $limit: limite }
    ]);
    res.json(data);
});

// 5. Horas pico (Filtro dia/hora)
app.get('/api/peaks', async (req, res) => {
    const { dia, hora } = req.query;
    let pipeline = [{ $project: { dia: { $dayOfWeek: "$start time" }, hora: { $hour: "$start time" } } }];
    
    let match = {};
    if (dia) match.dia = parseInt(dia);
    if (hora) match.hora = parseInt(hora);
    if (dia || hora) pipeline.push({ $match: match });

    pipeline.push(
        { $group: { _id: { dia: "$dia", hora: "$hora" }, total: { $sum: 1 } } },
        { $sort: { total: -1 } }
    );
    const data = await Trip.aggregate(pipeline);
    res.json(data);
});

// Iniciar servidor
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Servidor corriendo en puerto ${PORT}`));