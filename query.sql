USE u349685578_webscraping;

DROP TABLE IF EXISTS `data`;
CREATE TABLE `data` (
  `ID` int null,
  `Fecha` datetime null,
  `Precio` varchar(20) null,
  `Año Modelo` varchar(5) null,
  `Kilometraje` varchar(13) null,
  `Transmisión` varchar(40) null,
  `Combustible` varchar(20) null,
  `Cilindrada` varchar(11) null,
  `Categoría` varchar(20) null,
  `Marca` varchar(15) null,
  `Modelo` varchar(20) null,
  `Año de fabricación` varchar(5) null,
  `Número de puertas` varchar(2) null,
  `Tracción` varchar(15) null,
  `Color` varchar(15) null,
  `Número cilindros` varchar(2) null,
  `Placa` varchar(5) null,
  `URL` varchar(150) null
);

