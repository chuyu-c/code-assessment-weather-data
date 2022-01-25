SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';


-- -----------------------------------------------------
-- Schema public
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `public` DEFAULT CHARACTER SET utf8 ;
USE `public` ;

-- -----------------------------------------------------
-- Table `public`.`weather_daily_table`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `public`.`weather_daily_table` (
  `datetime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `name` VARCHAR(45) NOT NULL,
  `City` VARCHAR(45) NULL DEFAULT NULL,
  `State` VARCHAR(45) NULL DEFAULT NULL,
  `Country` VARCHAR(45) NULL DEFAULT NULL,
  `tempmax` FLOAT(45) NULL DEFAULT NULL,
  `tempmin` FLOAT(45) NULL DEFAULT NULL,
  `temp` FLOAT(45) NULL DEFAULT NULL,
  `feelslikemax` FLOAT(45) NULL DEFAULT NULL,
  `feelslikemin` FLOAT(45) NULL DEFAULT NULL,
  `feelslike` FLOAT(45) NULL DEFAULT NULL,
  `conditions` VARCHAR(45) NULL DEFAULT NULL,
  `description` VARCHAR(45) NULL DEFAULT NULL,
  PRIMARY KEY (`timestamp`,`name`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;