// MongoDB Playground
// Use Ctrl+Space inside a snippet or a string literal to trigger completions.

// The current database to use.
use("sample_mflix");

// Ejercicio 1: Haz una consulta que muestre cuántas películas se lanzaron en cada año.
db.movies.aggregate([
  {
    $group: {
      _id: "$year",
      cantidad_lanzamientos: { $sum: 1 },
    },
  },
  {
    $project: {
      _id: 0,
      año_lanzamiento: "$_id",
      cantidad_peliculas: "$cantidad_lanzamientos",
    },
  },
  { $sort: { año_lanzamiento: 1 } },
]);

//Ejercicio 2: Encuentra la calificación promedio de IMDb para películas en cada género.
use("sample_mflix");
db.movies.aggregate([
  { $unwind: "$genres" },
  {
    $group: {
      _id: "$genres",
      promedio_IMDb: { $avg: "$imdb.rating" },
    },
  },
  {
    $project: {
      _id: 0,
      genero: "$_id",
      promedio_IMDb: "$promedio_IMDb",
    },
  },
  {
    $sort: { genero: 1 },
  },
]);

//Ejercicio 3: Lista las 5 películas con la calificación IMDb más alta.
use("sample_mflix");
db.movies.aggregate([
  {
    $match: {
      "imdb.rating": { $nin: [null, ""] },
    },
  },
  {
    $sort: { "imdb.rating": -1 },
  },
  {
    $project: {
      _id: 0,
      pelicula: "$title",
      calificacion: "$imdb.rating",
    },
  },
  {
    $limit: 5,
  },
]);

//Ejercicio 4: Determina el número total de películas y la duración promedio para cada director.
use("sample_mflix");
db.movies.aggregate([
  {
    $unwind: "$directors",
  },
  {
    $group: {
      _id: "$directors",
      total_peliculas: { $sum: 1 },
      duracion_prom: { $avg: "$runtime" },
    },
  },
  {
    $project: {
      _id: 0,
      director: "$_id",
      total_peliculas: "$total_peliculas",
      duracion_prom: { $round: ["$duracion_prom", 2] },
    },
  },
  {
    $sort: { total_peliculas: -1, director: 1 },
  },
]);

//Ejercicio 5: Encuentra la distribución de películas basadas en su clasificación MPAA.
use("sample_mflix");
db.movies.aggregate([
  {
    $group: {
      _id: null,
      total_peliculas: { $sum: 1 },
      clasificaciones: { $push: "$rated" },
    },
  },
  { $unwind: "$clasificaciones" },
  {
    $match: {
      clasificaciones: { $in: ["G", "PG", "PG-13", "R", "NC-17"] },
    },
  },
  {
    $group: {
      _id: "$clasificaciones",
      cantidad: { $sum: 1 },
      total_peliculas: { $first: "$total_peliculas" },
    },
  },
  {
    $project: {
      _id: 0,
      clasificacion: "$_id",
      porcentaje_peliculas: {
        $round: [
          { $multiply: [{ $divide: ["$cantidad", "$total_peliculas"] }, 100] },
          2,
        ],
      },
    },
  },
  {
    $sort: {
      porcentaje_peliculas: -1,
    },
  },
]);

//Ejercicio 6: Encuentra los 3 países que producen el mayor número de películas.
use("sample_mflix");
db.movies.aggregate([
  { $unwind: "$countries" },
  {
    $group: {
      _id: "$countries",
      cantidad_peliculas: { $sum: 1 },
    },
  },
  { $sort: { cantidad_peliculas: -1 } },
  {
    $project: {
      _id: 0,
      pais: "$_id",
      cantidad_peliculas: "$cantidad_peliculas",
    },
  },
  { $limit: 3 },
]);

//Ejercicio 7: Para películas lanzadas después del 2000, calcula el número promedio de miembros del reparto.
use("sample_mflix");
db.movies.aggregate([
  {
    $match: {
      year: { $gt: 2000 },
      cast: { $exists: true, $ne: null },
    },
  },
  { $unwind: "$cast" },
  {
    $group: {
      _id: "$title",
      reparto: { $sum: 1 },
    },
  },
  {
    $group: {
      _id: null,
      promedio_miembros_reparto: { $avg: "$reparto" },
    },
  },
  {
    $project: {
      _id: 0,
      promedio_miembros_reparto: { $round: ["$promedio_miembros_reparto", 0] },
    },
  },
]);

//Ejercicio 8: Determina la calificación promedio de IMDb para las películas y su respectivo número de comentarios.
use("sample_mflix");
db.movies.aggregate([
  {
    $match: { "imdb.rating": { $nin: [null, ""] } },
  },
  {
    $lookup: {
      from: "comments",
      localField: "_id",
      foreignField: "movie_id",
      as: "comments",
    },
  },
  {
    $project: {
      imdb_rating: "$imdb.rating",
      comment_count: { $size: "$comments" },
    },
  },
  {
    $match: { comment_count: { $gt: 0 } },
  },
  {
    $limit: 100,
  },
  {
    $group: {
      _id: null,
      promedio_IMDb: { $avg: "$imdb_rating" },
      promedio_comentarios: { $avg: "$comment_count" },
    },
  },
  {
    $project: {
      _id: 0,
      promedio_IMDb: 1,
      promedio_comentarios: 1,
    },
  },
]);

//Ejercicio 9: Encuentra el número total de películas en las que cada usuario ha comentado.
use("sample_mflix");
db.comments.aggregate([
  {
    $group: {
      _id: "$name",
      peliculas_comentadas: {
        $sum: 1,
      },
    },
  },
  {
    $project: {
      _id: 0,
      usuario: "$_id",
      peliculas_comentadas: "$peliculas_comentadas",
    },
  },
  {
    $sort: { peliculas_comentadas: -1, usuario: 1 },
  },
]);

//Ejercicio 10: Encuentra películas del género "Western" y sus respectivos comentarios antes del "2020-01-01".
use("sample_mflix");
db.movies.aggregate([
  { $unwind: "$genres" },
  {
    $match: {
      genres: { $eq: "Western" },
      released: { $lt: ISODate("2020-01-01T00:00:00Z") },
    },
  },
  {
    $lookup: {
      from: "comments",
      localField: "_id",
      foreignField: "movie_id",
      as: "comments",
    },
  },
  {
    $project: {
      _id: 0,
      title: "$title",
      genres: "$genres",
      released: "$released.date",
      comment_count: { $size: "$comments" },
      comments: "$comments",
    },
  },
]);
