CREATE TABLE `organization_data` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
`name` varchar(300) DEFAULT NULL,
`OGRN` varchar(300) DEFAULT NULL,
 `OKPO` varchar(300) DEFAULT NULL,
`status` enum('действующая','в процессе ликвидации','ликвидирована') DEFAULT NULL,
`register_date` date DEFAULT NULL,
`auth_capital` decimal(40,0) DEFAULT NULL,
PRIMARY KEY (`id`)
) 
ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci