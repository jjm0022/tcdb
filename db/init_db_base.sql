SET FOREIGN_KEY_CHECKS=0;
DROP TABLE IF EXISTS regions;
CREATE TABLE regions (
  id int PRIMARY KEY AUTO_INCREMENT,
  long_name varchar(255) NOT NULL UNIQUE,
  short_name varchar(6) NOT NULL UNIQUE,
  region_char varchar(1) NOT NULL UNIQUE,
  last_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO regions (long_name, short_name, region_char) 
VALUES 
  ("North Atlantic", "AL", "l"),
  ("East Pacific", "EP", "e"),
  ("Central Pacific", "CP", "c"),
  ("West Pacific", "WP", "w"),
  ("Indian Ocean", "IO", "i"),
  ("Southern Hemisphere", "SH", "s");

DROP TABLE IF EXISTS models;
CREATE TABLE models (
  id int PRIMARY KEY AUTO_INCREMENT,
  long_name varchar(255) NOT NULL,
  short_name varchar(10) NOT NULL,
  last_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS data_sources;
CREATE TABLE data_sources (
  id int PRIMARY KEY AUTO_INCREMENT,
  long_name varchar(255) NOT NULL,
  short_name varchar(10),
  last_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO data_sources (long_name, short_name)
VALUES
  ("National Hurricane Center ATCF", "NHC"),
  ("National Centers for Environmantal Prediction", "NCEP"),
  ("Climate Forecast Applications Network", "CFAN"),
  ("Joint Typhoon Weather Center", "JTWC"),
  ("Japan Meteorological Agency", "JMA");

DROP TABLE IF EXISTS forecasts;
CREATE TABLE forecasts (
  id int PRIMARY KEY AUTO_INCREMENT,
  data_source_id int NOT NULL,
  model_id int NOT NULL,
  region_id int NOT NULL,
  datetime_utc datetime NOT NULL,
  run_id varchar(255) NOT NULL,
  last_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS storms;
CREATE TABLE storms (
  id int PRIMARY KEY AUTO_INCREMENT,
  annual_id int NOT NULL,
  region_id int NOT NULL,
  nhc_number int NOT NULL,
  nhc_id varchar(10) NOT NULL,
  season int NOT NULL,
  `start_date` datetime NOT NULL,
  end_date datetime,
  `status` varchar(10) NOT NULL,
  `name` varchar(25),
  start_lat float NOT NULL,
  start_lon float NOT NULL,
  run_id varchar(255) NOT NULL,
  last_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS observations;
CREATE TABLE observations (
  id int PRIMARY KEY AUTO_INCREMENT,
  storm_id int NOT NULL,
  datetime_utc datetime NOT NULL,
  latitude float NOT NULL,
  longitude float NOT NULL,
  intensity_kts float NOT NULL,
  mslp_mb float,
  r34_ne int,
  r34_se int,
  r34_sw int,
  r34_nw int,
  r50_ne int,
  r50_se int,
  r50_sw int,
  r50_nw int,
  r64_ne int,
  r64_se int,
  r64_sw int,
  r64_nw int,
  pouter_mb int,
  router_nmi int,
  rmw_nmi int,
  run_id varchar(255) NOT NULL,
  last_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS tracks;
CREATE TABLE tracks (
  id int PRIMARY KEY AUTO_INCREMENT,
  storm_id int NOT NULL,
  forecast_id int NOT NULL,
  ensemble_number int NOT NULL,
  run_id varchar(255) NOT NULL,
  last_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS steps;
CREATE TABLE steps (
  id int PRIMARY KEY AUTO_INCREMENT, 
  track_id int NOT NULL,
  hour int NOT NULL,
  latitude float NOT NULL,
  longitude float NOT NULL,
  intensity_kts float NOT NULL,
  mslp_mb float,
  run_id varchar(255) NOT NULL,
  last_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

ALTER TABLE storms ADD FOREIGN KEY (region_id) REFERENCES regions(id);

ALTER TABLE observations ADD FOREIGN KEY (storm_id) REFERENCES storms(id);

ALTER TABLE steps ADD FOREIGN KEY (track_id) REFERENCES tracks(id);

ALTER TABLE tracks ADD FOREIGN KEY (forecast_id) REFERENCES forecasts(id);
ALTER TABLE tracks ADD FOREIGN KEY (storm_id) REFERENCES storm(id);

ALTER TABLE forecasts ADD FOREIGN KEY (region_id) REFERENCES regions(id);
ALTER TABLE forecasts ADD FOREIGN KEY (data_source_id) REFERENCES data_sources(id);
ALTER TABLE forecasts ADD FOREIGN KEY (model_id) REFERENCES models(id);

CREATE UNIQUE INDEX storms_index ON storms(start_date, nhc_id);

CREATE UNIQUE INDEX observations_index ON observations(storm_id, datetime_utc);

CREATE UNIQUE INDEX forecasts_index ON forecasts(region_id, data_source_id, model_id, datetime_utc);

CREATE UNIQUE INDEX tracks_index ON tracks(forecast_id, storm_id, ensemble_number);

CREATE UNIQUE INDEX steps_index ON steps(track_id, hour);

SET FOREIGN_KEY_CHECKS=1;

LOAD DATA INFILE '/var/lib/mysql-files/models_table.csv' INTO TABLE models FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
