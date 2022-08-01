-- one complete statement per line
SET time_zone = "+00:00"
CREATE TABLE IF NOT EXISTS `test` ( `id` int(11) NOT NULL AUTO_INCREMENT, `name` varchar(50) NOT NULL, `value` varchar(2500) DEFAULT NULL, `lastModify` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(), PRIMARY KEY (`id`), UNIQUE KEY `name` (`name`) ) ENGINE=InnoDB DEFAULT CHARSET=latin1
INSERT INTO `test` (`name`, `value`) VALUES ('key', 'value')
