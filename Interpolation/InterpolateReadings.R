#################################
# LIBRARIES
#################################
library(readr)
library(tidyverse)
library(phylin)
library(sf)

path_to_data <- ""

# Import Sensor data
data <- read_csv(paste0(path_to_data, "data.csv"))
data$Date = as.Date(data$ReadingDateTimeLocal)


####
# Create interpolation grid with CE Tract map
####

chicago = st_read("Interpolation/tracts.shp")
grid <- chicago %>% st_make_grid(n = c(200,400), what="centers") %>%
  st_sf() %>% st_join(y=chicago, left=FALSE) %>%
  select(namelsad10) %>% rename(Tract = namelsad10)
pts = st_coordinates(grid) %>% as.data.frame()
colnames(pts) = c("Longitude", "Latitude")


#' Interpolation function
#'
#' @param data tibble of locations with columns for different readings
#' @param newdata a grid to interpolate to
#' @param var_names the columns to interpolate
#'
idw_interp <- function(data, newdata, var_names){
  pts = st_coordinates(newdata) %>% as.data.frame()
  colnames(pts) = c("Longitude", "Latitude")

  for (i in 1:length(var_names)){
    interps = idw(values=unlist(data[, var_names[i]]),
                  coords=cbind(data$Longitude, data$Latitude),
                  grid=pts)
    newdata[, var_names[i]] = interps
  }
  newdata$Longitude = pts$Longitude
  newdata$Latitude = pts$Latitude

  return(newdata)
}

# Interpolate readings to the grid

var_names = colnames(data)[-c(1:3)]
idw = idw_interp(data, grid, var_names=var_names)

# Summarize interpolated readings to the tract level
tract_readings = idw %>% as_tibble() %>% select(-geometry) %>%
  group_by(Tract) %>% summarise(across(everything(), mean, na.rm=TRUE))
tract_readings = tract_readings %>%
  left_join(chicago %>% select(namelsad10), by=c("Tract" = "namelsad10"))

write_csv(idw, file=paste0(path_to_data, "interpolated_data.csv"),
          row.names=F)
st_write(tract_readings, file=paste0(path_to_data, "tracts.geojson"),
         row.names=F)