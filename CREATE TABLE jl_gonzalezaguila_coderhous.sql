CREATE TABLE jl_gonzalezaguila_coderhouse.cuidad_url(
   id int NOT NULL, /* Cannot be overridden */
   url varchar(255)
 );

CREATE TABLE jl_gonzalezaguila_coderhouse.cuidad(
   id_cuidad int NOT NULL, /* Cannot be overridden */
   cuidad varchar(255),
   pais varchar(255)
 );
 

 CREATE TABLE jl_gonzalezaguila_coderhouse.clima(
   id_cuidad int NOT NULL, /* Cannot be overridden */
   status int,
   location varchar(255),
   fecha int,
   dia_semana varchar(255),
   mes varchar(255),
   symbol_value int,
   symbol_description varchar(255),
   symbol_value2 int,
   symbol_description2 varchar(255),
   tempmin float,
   tempmax float,
   rain float,
   humidity float,
   pressure int,
   snowline int,
   uv_index_max int,
   local_time varchar(255),
   local_time_offset int
 );
 
CREATE TABLE jl_gonzalezaguila_coderhouse.cuidad_moon(
   id_cuidad int NOT NULL, /* Cannot be overridden */
   fecha int,
   in_moon varchar(255),
   out_moon varchar(255),
   lumi_moon float,
   desc_moon varchar(255),
   symbol_moon int
 );
 

CREATE TABLE jl_gonzalezaguila_coderhouse.cuidad_sun(
   id_cuidad int NOT NULL, /* Cannot be overridden */
   fecha int,
   in_sun varchar(255),
   mid_sun varchar(255),
   out_sun varchar(255)
 );
 

CREATE TABLE jl_gonzalezaguila_coderhouse.cuidad_wind(
   id_cuidad int NOT NULL, /* Cannot be overridden */
   fecha int,
   speed_wind int,
   symbol_wind int,
   symbolB_wind int,
   gusts_wind int
 );
 
CREATE TABLE jl_gonzalezaguila_coderhouse.cuidad_horas(
   id_cuidad int NOT NULL, /* Cannot be overridden */
   fecha int,
   interval	varchar(255),
   temp	int,
   symbol_value	int,
   symbol_description varchar(255),	
   symbol_value2 int,
   symbol_description2	varchar(255),
   wind	varchar(255),
   rain	float,
   humidity	int,
   pressure	int,
   clouds float,
   snowline	int,
   windchill int,
   uv_index int
 );