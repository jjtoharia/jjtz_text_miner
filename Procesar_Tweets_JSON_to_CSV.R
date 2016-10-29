# options(echo = FALSE) # ECHO OFF
print('#######################################################################################################')
print('# jjtz_text_miner')
print('#######################################################################################################')
### Inicialización (setwd() y rm() y packages):

# setwd(getwd())
try(setwd('C:/Users/jtoharia/Dropbox/AFI_JOSE/Proyecto Final/TextMining'), silent=TRUE)
try(setwd('C:/Personal/Dropbox/AFI_JOSE/Proyecto Final/TextMining'), silent=TRUE)
rm(list = ls()) # Borra todos los elementos del entorno de R.

# # install.packages("ROAuth")
# library(ROAuth)
# install.packages("streamR")
library(bitops) # Requerido por RCurl
library(RCurl) # Requerido por streamR
library(rjson) # Requerido por streamR
library(streamR)
# install.packages("RJSONIO")
suppressMessages(library(RJSONIO))

# install.packages("stringr")
library(stringr)

# Process in parallel:
# install.packages("doParallel")
suppressMessages(library(foreach))
library(iterators)
library(parallel)
library(doParallel)
# # Process in parallel: Ejemplo de uso:
# cl <- makeCluster(detectCores(), type='PSOCK') # library(doParallel) [turn parallel processing on]
# registerDoParallel(cl) # library(doParallel) [turn parallel processing on]
# registerDoSEQ() # library(doParallel) [turn parallel processing off and run sequentially again]
# #

# ##################################################
# ## Funciones útiles:
# ##################################################
source("funciones_utiles.R")

# ##################################################
# ## Funciones:
# ##################################################

finaliza <- function(mi_tiempo_tot, i_sProyecto.s = Proyecto.s) # Guarda estadísticas de este proceso
{
  ## # NOTA: El fichero escrito por esta función se puede leer así:
  ## mi_tiempo_tot <- read.csv2("Procesar_Tweets_JSON_to_CSV.CSV", stringsAsFactors = FALSE)
  ## mi_tiempo_tot$fecha <- as.POSIXct(mi_tiempo_tot$fecha, origin = "1970-01-01 00:00.00 UTC")
  if(nrow(mi_tiempo_tot) == 0)    return(0) # Ok.
  # Guardamos las estadísticas (fecha, proyecto, fich, proc, segundos, to_str)
  # en el CSV "Proyecto.CSV":
  local.Proyecto.s <- i_sProyecto.s
  while(file.exists(paste0(local.Proyecto.s, '..CSV')))
  {
    local.Proyecto.s <- paste0(local.Proyecto.s, '.') # Buscamos el último por si hubo algún error en algún momento
  }
  b_CsvExiste <- file.exists(paste0(local.Proyecto.s, '.CSV'))
  
  # paste(names(mi_tiempo_tot), collapse = ',')
  mi_stat <- mi_tiempo_tot[, c("fecha", "fich", "proc", "segundos", "to_str")]
  mi_stat$proyecto <- i_sProyecto.s
  # Reordenamos columnas:
  mi_stat <- subset(mi_stat, select=c(fecha, proyecto, fich, proc, segundos, to_str))
  
  nErrCount <- 0
  while(inherits(
    try(
      write.table(
        x = mi_stat,
        file = paste0(local.Proyecto.s, '.CSV'),
        append = TRUE,
        quote = TRUE,
        sep = ";",
        dec = ",",
        row.names = FALSE, # row.names = TRUE, # es como write.csv2()
        col.names = !b_CsvExiste, # ifelse(b_CsvExiste, FALSE, NA), # es como write.csv2()
        qmethod = "double"
        #, fileEncoding = "UTF-8"
      ) # es como write.csv2() pero con append=TRUE y fileEnconding=utf8
      , silent = FALSE)
    , "try-error"))
  {
    nErrCount <- nErrCount + 1
    if(nErrCount > 5)
    {
      print('ERROR: Demasiados errores... Terminamos!')
      stop(paste0(Sys.time(), ' - ', 'ERROR: Demasiados errores intentando escribir en <', local.Proyecto.s, '.CSV>. Detenemos el proceso...'))
    }
    local.Proyecto.s <- paste0(local.Proyecto.s, '.')
    b_CsvExiste <- file.exists(paste0(local.Proyecto.s, '.CSV'))
    print(paste0(Sys.time(), ' - ', 'WARNING: Probamos a continuar en el fichero <', local.Proyecto.s, '.CSV>...'))
  }
  return(0) # Ok.
}

procesar_campos_tweets <- function(tweets.list = NULL, json_tweets_fullpathname = NULL, b_errOnNew = TRUE) # Procesamos y, si hay, añadimos campos a partir de tweets.list
{
  # json_tweets_fullpathname <- "C:/Users/Jose/Documents/jjtz_text_miner/JSON/Capturar_Tweets_2016_10_27-15_17_24.json"; tweets.list <- NULL; b_errOnNew <- FALSE;
  stopifnot(!is.null(tweets.list) | !is.null(json_tweets_fullpathname)) # Ambos no pueden ser nulos!
  # Campos a utilizar a priori (esta variable es la que se devuelve):
  CamposTweet <- read.csv2(file = 'CamposTweet.csv', stringsAsFactors = F, blank.lines.skip = TRUE)
  CamposTweet <- unique(as.vector(t(CamposTweet))) # Convertimos a vector unique
  # Cargamos 'CamposTweet_All.csv':
  CamposTweet.All <- read.csv2(file = 'CamposTweet_All.csv', stringsAsFactors = F, blank.lines.skip = TRUE)
  CamposTweet.All <- unique(as.vector(t(CamposTweet.All))) # Convertimos a vector unique

  # Creamos lista de campos a partir de la lista de tweets;
  if(is.null(tweets.list))  tweets.list <- readTweets(json_tweets_fullpathname, verbose = FALSE)
  Campos.tweets.list <- unique(names(unlist(tweets.list)))
  
  CamposNuevos <- Campos.tweets.list[!(Campos.tweets.list %in% CamposTweet.All)]
  if(length(CamposNuevos) == 0)
    return(CamposTweet); # Ok. No hay campos nuevos. No hacemos nada.

  # # Contenido de los campos nuevos:
  # print(unlist(tweets.list)[names(unlist(tweets.list)) %in% CamposNuevos])
  
  if(!b_errOnNew){
    print(paste0('Se han encontrado ', length(CamposNuevos), ' nuevo(s) campo(s) de tweets. Actualizamos fichero <CamposTweet_All.csv>...'))
  } else {
    sTmp <- paste0('json_tweets_fullpathname <- "', json_tweets_fullpathname, '"; tweets.list <- NULL; b_errOnNew <- FALSE; Cf. procesar_campos_tweets()')
    stop(paste0(length(CamposNuevos), ' nuevo(s) campo(s) de tweets. [', sTmp, ']'))
  }
  
  # Añadimos nuevos campos a 'CamposTweet_All.csv':
  CamposTweet.All <- unique(c(CamposTweet.All, CamposNuevos))
  write.csv2(x = data.frame(NombreCampo=CamposTweet.All), file = 'CamposTweet_All.csv', row.names = FALSE)
  
  # Campos a excluir: quitamos algunos campos de los tweets:
  CamposTweet.Excl.0 <- c('id' # Usaremos id_str en su lugar
                          , CamposTweet.All[grep(pattern = "truncated", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "protected", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "verified", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "retweeted", x = CamposTweet.All)] # FALSE en Stream
                          , CamposTweet.All[grep(pattern = "favorited", x = CamposTweet.All)] # FALSE en Stream
                          , CamposTweet.All[grep(pattern = "retweet_count", x = CamposTweet.All)] # 0 en Stream
                          , CamposTweet.All[grep(pattern = "favorite_count", x = CamposTweet.All)] # 0 en Stream
                          , CamposTweet.All[grep(pattern = "contributor", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "translator", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "color", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "_tile", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "banner", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "default", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "text_range", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "possibly_sensitive", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "filter_level", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "withheld_", x = CamposTweet.All)]
                          , CamposTweet.All[grep(pattern = "display_url$", x = CamposTweet.All)][ # display_url si hay otro igual con expanded_url
                            CamposTweet.All[grep(pattern = "display_url$", x = CamposTweet.All)] %in% str_replace(
                              CamposTweet.All[grep(pattern = "expanded_url$", x = CamposTweet.All)],
                              pattern = "expanded_url", replacement = "display_url")
                            ]
                          , CamposTweet.All[grep(pattern = "\\.url$", x = CamposTweet.All)][ # .url si hay otro igual con .expanded_url
                            CamposTweet.All[grep(pattern = "\\.url$", x = CamposTweet.All)] %in% str_replace(
                              CamposTweet.All[grep(pattern = "\\.expanded_url$", x = CamposTweet.All)],
                              pattern = ".expanded_url", replacement = ".url")
                            ]
                          , CamposTweet.All[grep(pattern = "_url_https$", x = CamposTweet.All)][ # _url_https si hay otro igual con _url
                            CamposTweet.All[grep(pattern = "_url_https$", x = CamposTweet.All)] %in% str_replace(
                              CamposTweet.All[grep(pattern = "_url$", x = CamposTweet.All)],
                              pattern = "_url", replacement = "_url_https")
                            ]
                          , CamposTweet.All[grep(pattern = "_id$", x = CamposTweet.All)][ # _id si hay otro igual con _id_str
                            CamposTweet.All[grep(pattern = "_id$", x = CamposTweet.All)] %in% str_replace(
                              CamposTweet.All[grep(pattern = "_id_str$", x = CamposTweet.All)],
                              pattern = "_id_str", replacement = "_id")
                            ]
                          , CamposTweet.All[ grep(pattern = "\\.id$", x = CamposTweet.All)][ # .id si hay otro igual con .id_str
                            CamposTweet.All[grep(pattern = "\\.id$", x = CamposTweet.All)] %in% str_replace(
                              CamposTweet.All[grep(pattern = "\\.id_str$", x = CamposTweet.All)],
                              pattern = ".id_str", replacement = ".id")
                            ]
                          , CamposTweet.All[ grep(pattern = "\\.text$", x = CamposTweet.All)][ # .text si hay otro igual con .full_text
                            CamposTweet.All[grep(pattern = "\\.text$", x = CamposTweet.All)] %in% str_replace(
                              CamposTweet.All[grep(pattern = "\\.full_text$", x = CamposTweet.All)],
                              pattern = ".full_text", replacement = ".text")
                            ]
                          , CamposTweet.All[grep(pattern = "contributors\\.|contributors_enable|contributors$", x = CamposTweet.All)] # Tabla aparte
                          , CamposTweet.All[grep(pattern = "entities\\.", x = CamposTweet.All)] # Tabla aparte
                          , CamposTweet.All[grep(pattern = "place.attributes\\.", x = CamposTweet.All)] # Tabla aparte
                          , CamposTweet.All[grep(pattern = "annotations\\.", x = CamposTweet.All)] # Tabla aparte (Future/beta)
                          , CamposTweet.All[grep(pattern = "scopes\\.|scopes$", x = CamposTweet.All)] # Tabla aparte (used by Twitter's Promoted Products)
                          , CamposTweet.All[grep(pattern = "geo\\.|geo$", x = CamposTweet.All)] # deprecated (use coordinates instead)
                          , CamposTweet.All[grep(pattern = "\\.following", x = CamposTweet.All)] # deprecated
  )
  CamposTweet.Excl.0 <- unique(CamposTweet.Excl.0) # Convertimos a vector unique, por si acaso
  # Ahora actualizamos, si hace falta, 'CamposTweet_Excl.csv':
  CamposTweetExcl <- read.csv2(file = 'CamposTweet_Excl.csv', stringsAsFactors = F, blank.lines.skip = TRUE)
  CamposTweetExcl <- unique(as.vector(t(CamposTweetExcl))) # Convertimos a vector unique
  CamposNuevos <- unique(CamposTweet.Excl.0[!(CamposTweet.Excl.0 %in% CamposTweetExcl)])
  if(length(CamposNuevos) != 0)
  {
    print(paste0('Se han encontrado ', length(CamposNuevos), ' nuevo(s) campo(s) de tweets a excluir. Actualizamos fichero <CamposTweet_Excl.csv>...'))
    CamposTweetExcl <- unique(c(CamposTweetExcl, CamposNuevos)) # Convertimos a vector unique, por si acaso
    write.csv2(x = data.frame(NombreCampoExcl=CamposTweetExcl), file = 'CamposTweet_Excl.csv', row.names = FALSE)
  }
  
  # Finalmente, actualizamos, si hace falta, 'CamposTweet.csv':
  # NOTA: Esto además genera un Warning()
  CamposTweet.0 <- CamposTweet.All[!(CamposTweet.All %in% CamposTweetExcl)]
  CamposNuevos <- CamposTweet.0[!(CamposTweet.0 %in% CamposTweet)]
  if(length(CamposNuevos) != 0)
  {
    print(paste0('Se han encontrado ', length(CamposNuevos), ' nuevo(s) campo(s) de tweets. Actualizamos fichero <CamposTweet.csv>...'))
    CamposTweet <- unique(c(CamposTweet, CamposNuevos)) # Convertimos a vector unique, por si acaso
    write.csv2(x = data.frame(NombreCampo=CamposTweet), file = 'CamposTweet.csv', row.names = FALSE)
    warning(paste0('Se han encontrado ', length(CamposNuevos), ' nuevo(s) campo(s) de tweets. Se ha actualizado el fichero <CamposTweet.csv>'))
  } else
  {
    # Si no hay nuevos es que todo sigue igual o bien hemos quitado algunos campos.
    # Guardamos siempre:
    CamposTweet <- CamposTweet.0
    write.csv2(x = data.frame(NombreCampo=CamposTweet), file = 'CamposTweet.csv', row.names = FALSE)
  }
  return(CamposTweet); # Ok.
}

procesa_lista_tweets <- function(tweets.list, CamposTweet.vector) # Bucle principal para cada lista de tweets
{
  # Función definida internamente porque modifica variables "exteriores":
  procesa_lista_tweets.llena_df <- function(x, tweets.vector, CamposTweet.vector, tweets.df.int, numTweets)
  {
    campo.tweet <- tweets.vector[x]
    nbre.cpo <- names(campo.tweet)
    # print(nbre.cpo)
    if(nbre.cpo == CamposTweet.vector[1]) # "created_at" (NOTA: ¡siempre debería existir!)
    {
      numTweet <<- numTweet + 1 # Número de tweet (variable externa a la función!!!)
      tweets.df.int[numTweet, nbre.cpo] <<- campo.tweet
      if(numTweet %% 500 == 0)
        print(paste0(Sys.time(), ' - ', numTweet, ' tweets (', round(100 * numTweet / numTweets, 2), '%)'))
    }
    else if(nbre.cpo %in% CamposTweet.vector)
    {
      if(!is.na(tweets.df.int[numTweet, nbre.cpo]))
        stop(paste0('ERROR: Campo (', nbre.cpo,') repetido en el mismo tweet (', numTweet,' - created_at: ', tweets.df.int[numTweet, "created_at"],').'))
      tweets.df.int[numTweet, nbre.cpo] <<- campo.tweet
    }
  }
  
  print(paste0(Sys.time(), ' - Contando campos de tweets...'))
  tweets.vector <- unlist(tweets.list)
  
  tweets.df.int <- data.frame()
  numCampos <- length(CamposTweet.vector)
  for(i in (1:numCampos))
  {
    # Esto no tarda mucho...
    tweets.df.int[,CamposTweet.vector[i]] <- character(0) # tweets.vector[names(tweets.vector)==CamposTweet.vector[i]][0]
  }
  numTweets <- sum(names(tweets.vector)==CamposTweet.vector[1]) # Todo tweet debería tener el campo 1 "created_at"
  # Dimensionamos data.frame con el número de tweets (todo == NA):
  tweets.df.int[numTweets,1] <- NA
  print(paste0(Sys.time(), ' - Rellenando ', numTweets, ' tweets (máx ', numCampos,' campos)...'))
  
  numTweet <- 0
  sapply(X = seq_along(tweets.vector), FUN = procesa_lista_tweets.llena_df,
         tweets.vector = tweets.vector, CamposTweet.vector = CamposTweet.vector, tweets.df.int = tweets.df.int, numTweets = numTweets,
         USE.NAMES = FALSE)
  print(paste('Número total de Tweets:', numTweet))
  if(numTweet != numTweets) print('Algo no ha ido bien. (numTweet != numTweets)')
  return(tweets.df.int)
}

procesa_contenido_campos <- function(tweets.df) # Procesamos algunos campos
{
  # Reducimos el contenido de algunos campos:
  tweets.df$source <- trimws(str_replace_all(tweets.df$source, pattern = "(<a href=\"https?://)(.*)(\" rel=\"nofollow\">)(.*)</a>", replacement = "\\4 (\\2)"), "both")
  # Creamos campo "jjtz_source" a partir de "source":
  tweets.df$jjtz_source <- str_replace_all(tweets.df$source, pattern = "(Twitter for *)(.*)( \\(w?w?w?.?twitter.com)(.*)", replacement = "\\2")
  tweets.df$jjtz_source <- str_replace_all(tweets.df$jjtz_source, pattern = "(Twitter for *)(BlackBerry).? \\(w?w?w?.?blackberry.com.*", replacement = "\\2")
  tweets.df$jjtz_source <- str_replace_all(tweets.df$jjtz_source, pattern = "(Twitter for *)(Nokia .*) \\(s?t?o?r?e?.?ovi.com.*", replacement = "\\2")
  tweets.df$jjtz_source <- str_replace_all(tweets.df$jjtz_source, pattern = "(Twitter for *)(Mac) \\(i?t?u?n?e?s?.?apple.com.*", replacement = "\\2")
  tweets.df$jjtz_source <- str_replace_all(tweets.df$jjtz_source, pattern = "(OS X.*) \\(w?w?w?.?apple.com.*", replacement = "\\1")
  tweets.df$jjtz_source <- str_replace_all(tweets.df$jjtz_source, pattern = "(Twitter )(Web) Client .*twitter.com.*", replacement = "\\2")
  tweets.df$jjtz_source <- str_replace_all(tweets.df$jjtz_source, pattern = "(Mobile Web)( \\()(.*)\\) \\(.*twitter.com\\)", replacement = "\\1 \\3")
  tweets.df$jjtz_source <- str_replace_all(tweets.df$jjtz_source, pattern = "  *", replacement = " ") # Quitamos espacios dobles
  # Marcamos las que consideramos "humanos":
  tweets.df$jjtz_source <- str_replace(tweets.df$jjtz_source, pattern = "^(Web|iPhone|Android|Android Tablets|Windows Phone|Windows|Mac|iPad|BlackBerry|Mobile Web .*|Nokia .*|OS X.*)$", replacement = "JJTZ \\1")
  sources.all <- sort(unique(tweets.df$jjtz_source))
  # sources.tmp <- sort(unique(tweets.df$jjtz_source[grep(".*\\(.*", tweets.df$jjtz_source, invert = TRUE)]))
  # NOTA: sources.tmp debería coincidir con sources.human (PENDIENTE?)
  sources.human <- sort(unique(tweets.df$jjtz_source[grep("^JJTZ .*", tweets.df$jjtz_source)]))
  sources.tabla <- c(
    sort(table(tweets.df$jjtz_source[tweets.df$jjtz_source %in% sources.human]), decreasing = TRUE)
    , RESTO = length(tweets.df$jjtz_source[!(tweets.df$jjtz_source %in% sources.human)])
  )
  # Mostramos estadísticas del campo "jjtz_source":
  print('jjtz_source stats:')
  print(sources.tabla)
  
  # Modificamos nombre de algunos campos por claridad:
  # "statuses_count" es el número de tweets (incluidos retweets)
  return(tweets.df)
}

procesar_tweets_json <- function(tweets_filename = "Prueba_tweets2",
                                 json_full_path = '/json/', csv_full_path = '/csv/',
                                 mi_tiempo_total = NULL)
{
  # Inicializamos variables:
  mi_tiempo_total <- init_tiempo(mi_tiempo_total, i_bResetTime = FALSE)
  if(endsWith(tweets_filename, '.json')) # Quitamos extensión ".json"
    tweets_filename <- substring(tweets_filename, 1, nchar(tweets_filename) - 5)
  # json_tweets_fullpathname
  json_tweets_fullpathname <- paste0(json_full_path, tweets_filename, '.json')
  stopifnot(file.exists(json_tweets_fullpathname))
  # ## Cargamos list "tweets.list" con readTweets(): # Con esto se pueden obtener algunas propiedades más de los tweets y es más rápido que parseTweets(), pero devuelve una lista.
  mi_tiempo <- system.time({
    tweets.list <- readTweets(tweets = json_tweets_fullpathname, verbose = TRUE)
  })
  mi_tiempo_total <- add_tiempo(mi_tiempo, "readTweets()", mi_tiempo_tot = mi_tiempo_total, fich = tweets_filename) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
  nTweets <- length(tweets.list)
  
  if(nTweets == 0)
  {
    # ## file.rename():
    mi_tiempo <- system.time({
      try(file.rename(from = json_tweets_fullpathname, to = paste0(json_full_path, tweets_filename, '_no_tweets.json'))
          , silent = TRUE) # En caso de fallo, volveremos a él, así que no quiero saberlo...
    })
    mi_tiempo_total <- add_tiempo(mi_tiempo, "file.rename()", mi_tiempo_tot = mi_tiempo_total, fich = tweets_filename) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
  } else
  {
    # ## procesar_campos_tweets():
    mi_tiempo <- system.time({
      CamposTweet <- procesar_campos_tweets(tweets.list = tweets.list, json_tweets_fullpathname = json_tweets_fullpathname)
    })
    mi_tiempo_total <- add_tiempo(mi_tiempo, "procesar_campos_tweets()", mi_tiempo_tot = mi_tiempo_total, fich = tweets_filename) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
    
    # ## procesa_lista_tweets():
    mi_tiempo <- system.time({
      tweets.df <- procesa_lista_tweets(tweets.list = tweets.list, CamposTweet = CamposTweet)
    })
    mi_tiempo_total <- add_tiempo(mi_tiempo, "procesa_lista_tweets()", mi_tiempo_tot = mi_tiempo_total, fich = tweets_filename) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
    
    # ## procesa_contenido_campos():
    mi_tiempo <- system.time({
      tweets.df <- procesa_contenido_campos(tweets.df)
    })
    mi_tiempo_total <- add_tiempo(mi_tiempo, "procesa_contenido_campos()", mi_tiempo_tot = mi_tiempo_total, fich = tweets_filename) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
    
    # ## guardar_df_full():
    mi_tiempo <- system.time({
      write.csv2(x = tweets.df, file = paste0(csv_full_path, tweets_filename, '_df_full.csv'), row.names = FALSE)
    })
    mi_tiempo_total <- add_tiempo(mi_tiempo, "guardar_df_full()", mi_tiempo_tot = mi_tiempo_total, fich = tweets_filename) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
  }
  
  # # ## CANCELAMOS ESTA PARTE PORQUE parseTweets() DA ERRORES Y NO GANAMOS NADA.
  # # ## Cargamos data.frame "tweets.df.simplif" con parseTweets(): # parse the json file and save to a data frame (Simplify = FALSE ensures that we include lat/lon information in that data frame)
  # mi_tiempo <- system.time({
  #   tweets.df.simplif <- parseTweets(json_tweets_fullpathname, simplify = FALSE, verbose = TRUE)
  # })
  # mi_tiempo_total <- add_tiempo(mi_tiempo, "parseTweets()", mi_tiempo_tot = mi_tiempo_total, fich = tweets_filename) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
  #
  # # ## guardar_df_simpl():
  # mi_tiempo <- system.time({
  #   write.csv2(x = tweets.df.simplif, file = paste0(csv_full_path, tweets_filename, '_df_simpl.csv'), row.names = FALSE)
  #   # ## guardar_df_simpl_sin_RT():
  #   tweets.df.simplif_sin_RT <- tweets.df.simplif[substr(tweets.df.simplif$text, 1, 3) != 'RT ',]
  #   write.csv2(x = tweets.df.simplif_sin_RT, file = paste0(csv_full_path, tweets_filename, '_df_simpl_sin_RT.csv'), row.names = FALSE)
  # })
  # mi_tiempo_total <- add_tiempo(mi_tiempo, "guardar_df_simpl()", mi_tiempo_tot = mi_tiempo_total, fich = tweets_filename) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
  # 
  # # ## CANCELAMOS ESTA PARTE PORQUE parseTweets() DA ERRORES Y NO GANAMOS NADA.
  
  # Devolvemos mi_tiempo_total:
  return(mi_tiempo_total)
}

procesar_tweets_json.Path <- function(i_sFamilia = 'Prueba_tweets',
                                      i_sPath = '/json/',
                                      i_sPathOut = '/csv/',
                                      i_bProcesarTodo = FALSE, i_bResetTime = FALSE, i_bPrint = FALSE,
                                      mi_tiempo_total = NULL,
                                      reg.AAAA_MM_DD_HH_MM_SS = '[0-9]{4}[_-][0-9]{1,2}[_-][0-9]{1,2}[_-][0-9]{1,2}[_-][0-9]{1,2}[_-][0-9]{1,2}')
{
  # Inicializamos variables:
  mi_tiempo_total <- init_tiempo(mi_tiempo_total, i_bResetTime = i_bResetTime)
  systime_ini_fun <- proc.time()
  fecha_ini_fun <- Sys.time()
  # Inicializamos parámetros:
  sPath <- file.path(i_sPath)
  if(i_sPathOut == FALSE)  sPathOut <- sPath  else  sPathOut <- file.path(i_sPathOut)
  # Ficheros json: "Proyecto"[_-]AAAA[_-]MM[_-]DD[_-]HH[_-]MM * .json
  regpat <- paste0('^', i_sFamilia, '[_-]?', reg.AAAA_MM_DD_HH_MM_SS)
  mis_ficheros <- list.files(path = sPath, ignore.case = TRUE,
                             pattern = paste0(regpat, '\\.json$'))
  nTot <- length(mis_ficheros)
  if(nTot > 0 & !i_bProcesarTodo)
  {
    # Ficheros ya procesados (csv) antes de empezar:
    mis_ficheros_ya_procesados <- list.files(path = sPathOut, ignore.case = TRUE, pattern = paste0('^', i_sFamilia, '.*\\.csv$'))
    # Ficheros json NO procesados todavía:
    mis_ficheros <- mis_ficheros[!(str_replace(mis_ficheros, '.json', '_df_full.csv') %in% mis_ficheros_ya_procesados)]
    # mis_ficheros <- mis_ficheros[!(str_replace(mis_ficheros, '.json', '_df_simpl.csv') %in% mis_ficheros_ya_procesados)]
    nTot <- length(mis_ficheros)
    if(nTot == 0)
    {
      to_str_nothing <- 'Ok. Todos los ficheros ya procesados.'
    }
  } else {
    to_str_nothing <- 'Ok. No hay ficheros.'
  }
  nOks <- 0
  nErrs <- 0
  if(nTot > 0)
  {
    print(paste0('- - - Procesando ', nTot, ' ficheros...'))
    # Dejamos el más reciente para el final, por si falla (puede estar siendo escrito todavía):
    mis_ficheros <- sort(mis_ficheros)
    
    # ==================================================================================
    # Paralelizamos:
    resAll.df <- foreach(f = mis_ficheros, .inorder=FALSE, .combine = rbind, .packages=c('stringr','streamR'),
                         .noexport = c("mi_tiempo_total"),
                         .export = c("procesar_tweets_json", "init_tiempo", "add_tiempo", "procesar_campos_tweets", "procesa_lista_tweets", "procesa_contenido_campos")
    ) %dopar%
    {
      systime_ini <- proc.time()
      fecha_ini <- Sys.time()
      print(paste0('- - Procesando fichero \'', f, '\'...')) #  [NOTA: con %dopar% esto se pierde...]
      mi_tiempo_tot_fich <- try(
        procesar_tweets_json(tweets_filename = f, json_full_path = i_sPath, csv_full_path = i_sPathOut)
        , silent = FALSE)
      if(inherits(mi_tiempo_tot_fich, "try-error"))
      {
        ret_val <- 99 # ERROR
        to_str.s <- geterrmessage()
      } else {
        ret_val <- 0 # 0 = OK
        to_str.s <- 'Ok.'
      }
      # Al menos un registro final por Fichero:
      res.df <- data.frame(proc = 'procesar_tweets_json()',
                           segundos = as.double((proc.time() - systime_ini)['elapsed']), 
                           to_str = to_str.s, fich = f, fecha = fecha_ini,
                           ret.val = ret_val, 
                           num.errs = ifelse(ret_val == 0, 0, 1),
                           stringsAsFactors = FALSE)
      if(ret_val == 0)
        if(nrow(mi_tiempo_tot_fich) != 0)
          res.df <- rbind(mi_tiempo_tot_fich, res.df)
      # res.df[nrow(res.df), ]$ret.val <- ret_val # Si hay error, sólo lo ponemos en la últ. fila
      # res.df[nrow(res.df), ]$num.errs <- ifelse(ret_val!=0, 1, 0) # Si hay error, sólo lo ponemos en la últ. fila
      # Mostramos los registros con error:
      if(sum(res.df$num.errs) != 0) #  [NOTA: con %dopar% esto se pierde...]
        print(res.df[res.df$num.errs != 0, 'to_str'])
      return(res.df) # ret_val de la función "foreach() %do%" (o foreach( %dopar%))
    }
    # ==================================================================================
    # Estadísticas finales:
    nOks <- sum(resAll.df$ret.val == 0)
    nErrs <- sum(resAll.df$num.errs)
    # Guardamos estadísticas en csv:
    finaliza(mi_tiempo_tot = resAll.df, i_sProyecto.s = Proyecto.s)
    if(i_bPrint)
    {
      print( setNames( aggregate(segundos ~ proc, resAll.df
                                 , function(a){ round(mean(as.double(a)), 2) }  )
                       , c('proc', 'Promedio(Segs)') ) )
      print( setNames( aggregate(segundos ~ proc, resAll.df
                                 , function(a){ round(sum(as.double(a)) / 60, 3) }  )
                       , c('proc', 'Total(Mins)') ) )
      if(nErrs > 0)
      { print(paste0(nErrs, ' ERRORES!')) }
      t_acc <- sum(as.double(resAll.df$segundos)) # Suma de tiempos calculados en cada proceso.
      t_real <- as.double(((proc.time() - systime_ini_fun)['elapsed'])) # Tiempo transcurrido desde el principio hasta el fin.
      print(paste0("Tiempo Total acumulado: ", round(t_acc / 60, 3), " minutos. [Ficheros Ok: ", nOks ,"/", nTot, " (", round(100*nOks/nTot,2) , "%)]"))
      print(paste0("Tiempo Total REAL: ", round(t_real / 60, 3), " minutos."))
      if(t_acc / t_real > 1)
      {
        print(paste0("NOTA: Parecen haberse usado aprox. ", round(t_acc / t_real, 1)," procesos en paralelo..."))
      }
    }
  } else
  {
    resAll.df <- data.frame(proc = 'procesar_tweets_json()', segundos = as.double((proc.time() - systime_ini_fun)['elapsed']), to_str = to_str_nothing, stringsAsFactors = FALSE)
    # Añadimos más campos a res.df:
    resAll.df$fich <- i_sFamilia
    resAll.df$fecha <- fecha_ini_fun
    resAll.df$ret.val <- 0
    resAll.df$num.errs <- 0
    print(to_str_nothing) # print('Ok. No hay ficheros.')
  }
  # Reordenamos columnas en resAll.df y en mi_tiempo_total:
  resAll.df <-       subset(resAll.df,       select=c(fecha,fich,ret.val,num.errs,proc,segundos,to_str))
  mi_tiempo_total <- subset(mi_tiempo_total, select=c(fecha,fich,ret.val,num.errs,proc,segundos,to_str))
  # Insertamos resAll.df en mi_tiempo_total:
  if(nrow(mi_tiempo_total) == 0){
    mi_tiempo_total <- resAll.df
  }else{
    mi_tiempo_total <- rbind(mi_tiempo_total, resAll.df)
  }
  # Finalmente, haya lo que haya en la última fila, ponemos el total de errores en num.errs:
  mi_tiempo_total[nrow(mi_tiempo_total), ]$num.errs <- nErrs # Si hay error, sólo lo ponemos en la últ. fila
  return(mi_tiempo_total)
}

procesar_tweets_json.Path.Familias <- function(sPath_json, sPath_csv, i_bResetTime = FALSE,
                                               reg.AAAA_MM_DD_HH_MM_SS = '[0-9]{4}[_-][0-9]{1,2}[_-][0-9]{1,2}[_-][0-9]{1,2}[_-][0-9]{1,2}[_-][0-9]{1,2}')
{
  mi_tiempo_total <- init_tiempo(mi_tiempo_tot = NULL, i_bResetTime = i_bResetTime)
  systime2_ini <- proc.time()
  fecha2_ini <- Sys.time()
  
  # Lista de "Familias":
  regpat <- reg.AAAA_MM_DD_HH_MM_SS
  AllFams <- list.files(path = sPath_json, pattern = paste0("(.*)[_-]?", regpat, '\\.json'))
  AllFams <- str_replace(AllFams, paste0("(.*)[_-](", regpat, ')\\.json'), "\\2 \\1")
  AllFams <- str_replace(AllFams, paste0("(.*)(", regpat, ')\\.json'), "\\2 \\1")
  AllFams <- sort(AllFams) # Ordenamos por fecha (los últimos al final)
  AllFams <- unique(str_replace_all(AllFams, "(.*) (.*)", "\\2"))
  nTot <- length(AllFams)
  if(nTot > 0)
  {
    print(paste0('- Procesando ', nTot, ' familias de ficheros...'))
    # ==================================================================================
    # Paralelizamos:
    resAll2.df <- foreach(Familia = AllFams, .inorder=FALSE, .combine = rbind, .packages=c('stringr','streamR','foreach'),
                          .noexport = c("mi_tiempo_total"),
                          .export = c("procesar_tweets_json.Path", "procesar_tweets_json", "init_tiempo", "add_tiempo", "procesar_campos_tweets", "procesa_lista_tweets", "procesa_contenido_campos")
    ) %do% # Dejamos el %dopar% para los ficheros...
    {
      systime_ini <- proc.time()
      fecha_ini <- Sys.time()
      print(paste0(' - - Procesando familia \'', Familia, '\'...')) #  [NOTA: con %dopar% esto se pierde...]
      mi_tiempo_tot_fich <- try(
        procesar_tweets_json.Path(i_sFamilia = Familia, i_bProcesarTodo = FALSE, i_sPath = sPath_json, i_sPathOut = sPath_csv)
        , silent = FALSE)
      if(inherits(mi_tiempo_tot_fich, "try-error"))
      {
        ret_val <- 99 # ERROR
        to_str.s <- geterrmessage()
      } else {
        ret_val <- 0 # 0 = OK
        to_str.s <- 'Ok.'
      }
      # Al menos un registro final por Familia:
      res.df <- data.frame(proc = 'procesar_tweets_json.Path()',
                           segundos = as.double((proc.time() - systime_ini)['elapsed']), 
                           to_str = to_str.s, fich = Familia, fecha = fecha_ini,
                           ret.val = ret_val, 
                           num.errs = ifelse(ret_val == 0, 0, 1),
                           stringsAsFactors = FALSE)
      if(ret_val == 0)
        if(nrow(mi_tiempo_tot_fich) != 0)
          res.df <- rbind(mi_tiempo_tot_fich, res.df)
      # res.df[nrow(res.df), ]$ret.val <- ret_val # Si hay error, sólo lo ponemos en la últ. fila
      # res.df[nrow(res.df), ]$num.errs <- ifelse(ret_val!=0, 1, 0) # Si hay error, sólo lo ponemos en la últ. fila
      # Mostramos los registros con error:
      if(sum(res.df$num.errs) != 0) #  [NOTA: con %dopar% esto se pierde...]
        print(res.df[res.df$num.errs != 0, 'to_str'])
      return(res.df) # ret_val de la función "foreach() %do%" (o foreach( %dopar%))
    }
    # ==================================================================================
  } else
  {
    resAll2.df <- data.frame(proc = 'procesar_tweets_json.Path.Familias()', segundos = as.double((proc.time() - systime_ini_fun)['elapsed']), to_str = to_str_nothing, stringsAsFactors = FALSE)
    # Añadimos más campos a res.df:
    resAll2.df$fich <- ""
    resAll2.df$fecha <- fecha_ini_fun
    resAll2.df$ret.val <- 0
    resAll2.df$num.errs <- 0
    print(to_str_nothing) # print('Ok. No hay ficheros.')
  }
  # Reordenamos columnas en resAll2.df y en mi_tiempo_total:
  resAll2.df <-      subset(resAll2.df,      select=c(fecha,fich,ret.val,num.errs,proc,segundos,to_str))
  mi_tiempo_total <- subset(mi_tiempo_total, select=c(fecha,fich,ret.val,num.errs,proc,segundos,to_str))
  # Insertamos resAll2.df en mi_tiempo_total:
  if(nrow(mi_tiempo_total) == 0){
    mi_tiempo_total <- resAll2.df
  }else{
    mi_tiempo_total <- rbind(mi_tiempo_total, resAll2.df)
  }
  return(mi_tiempo_total)
}

# ##################################################
# ## Inicio:
# ##################################################
########################################################################################################
Proyecto <- "Procesar Tweets JSON to CSV"
########################################################################################################
print(paste0(Sys.time(), ' - ', 'Proyecto = ', Proyecto))

Proyecto.s <- str_replace_all(Proyecto, "\\(|\\)| |:", "_") # Quitamos espacios, paréntesis, etc.

# Inicializamos variables:
sPath_json <- 'C:/Users/Jose/Documents/jjtz_text_miner/JSON/'
sPath_csv <- 'C:/Users/Jose/Documents/jjtz_text_miner/CSV/'

# NOTA: Dejamos un Core de la CPU "libre" para no "quemar" la máquina:
cl <- makeCluster(detectCores() - 1, type='PSOCK') # library(doParallel) [turn parallel processing on]
registerDoParallel(cl) # library(doParallel) [turn parallel processing on]

# mi_tiempo_total <- procesar_tweets_json.Path("Prueba_tweets", i_bProcesarTodo = TRUE)
# procesar_tweets_json('Prueba_tweets2016_10_19_01_36_23')

mi_tiempo_total <- procesar_tweets_json.Path.Familias(sPath_json, sPath_csv, i_bResetTime = TRUE)

# Para leer ficheros csv de tweets (ya procesados) [Cf. Procesar_Tweets_CSV.R]:
# mi_df <- read.csv2('Prueba_tweets2016_10_18_22_36_13_df_full.csv', stringsAsFactors = F, blank.lines.skip = TRUE)

# cleanup:
try(registerDoSEQ(), silent = TRUE) # library(doParallel) [turn parallel processing off and run sequentially again]
try(stopImplicitCluster(), silent = TRUE)
try(stopCluster(cl), silent = TRUE)
