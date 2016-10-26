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
p<-capture.output(
  library(RJSONIO)
  , type = c("message")) # Capturamos los mensajes que devuelve esta función (para no mostrarlos)

# install.packages("stringr")
library(stringr)

# Process in parallel:
# install.packages("doParallel")
p<-capture.output(
  library(foreach)
  , type = c("message")) # Capturamos los mensajes que devuelve esta función (para no mostrarlos)
library(iterators)
library(parallel)
library(doParallel)
# # Process in parallel: Ejemplo de uso:
# cl <- makeCluster(detectCores(), type='PSOCK') # library(doParallel) [turn parallel processing on]
# registerDoParallel(cl) # library(doParallel) [turn parallel processing on]
# registerDoSEQ() # library(doParallel) [turn parallel processing off and run sequentially again]
# #
rm(p)

# ##################################################
# ## Funciones útiles:
# ##################################################
check_Go <- function(Proyecto.s)
{
  if(!file.exists(paste0(Proyecto.s, '.GO')))
  {
    try(write("Buh!", file = paste0(Proyecto.s, '.stopped')), silent = TRUE)
    stop(paste0('Fichero <', Proyecto.s, '.GO', '> NO encontrado. Detenemos el proceso...'))
  }
}

# ### add_tiempo(): Devuelve data.frame con los tiempos de cada llamada:
# ###               data.frame( proc = character(), segundos = numeric(), to_str = character(), stringsAsFactors = FALSE)
add_tiempo <- function(mi_tiempo, proc_txt="", mi_tiempo_tot=NULL, b_print=TRUE, b_print_acum=TRUE, b_print_una_linea=TRUE)
{
  # Creamos data.frame "mi_tiempo_tot" si no se ha creado todavía:
  if(is.null(mi_tiempo_tot))
    mi_tiempo_tot <- data.frame(proc = character(), segundos = numeric(), to_str = character(), stringsAsFactors = FALSE)
  if(is.null(mi_tiempo)) 
    return(mi_tiempo_tot)
  # Creamos campo "segundos":
  mi_reg.s <- mi_tiempo[3]
  # Creamos campo "to_str":
  if(mi_reg.s < 60)
  { to_str <- paste0(round(mi_reg.s, 1), ' segundos')
  } else { if(mi_reg.s < 3600)
    { to_str <- paste0(round(mi_reg.s/60, 1), ' minutos')
    } else { if(mi_reg.s < 86400) { to_str <- paste0(round(mi_reg.s/3600, 1), ' horas') # 3600 * 24 = 86400
      } else { to_str <- paste0(round(mi_reg.s/86400, 2), ' días') } } }
  to_str <- paste0(trimws(paste0("Tiempo ", proc_txt), "right"), ": ", trimws(to_str, "left"))
  
  # Añadimos el registro usando la estructura de mi_tiempo_tot, por si tuviera otros campos:
  mi_tiempo_tot[nrow(mi_tiempo_tot) + 1, c('proc', 'segundos', 'to_str')] <- c(proc_txt, mi_reg.s, to_str)
    
  # Mostramos tiempo(s):
  print.s <- ""
  print_acc.s <- ""
  if(b_print)
  {
    print.s <- mi_tiempo_tot[nrow(mi_tiempo_tot),]$to_str
  }
  if(b_print_acum & nrow(mi_tiempo_tot) > 1)
  {
    mi_reg.s <- sum(mi_tiempo_tot$segundos)
    # Creamos campo "to_str":
    if(mi_reg.s < 60)
    { to_str <- paste0(round(mi_reg.s, 1), ' segundos')
    } else { if(mi_reg.s < 3600)
      { to_str <- paste0(round(mi_reg.s/60, 1), ' minutos')
      } else { if(mi_reg.s < 86400) { to_str <- paste0(round(mi_reg.s/3600, 1), ' horas') # 3600 * 24 = 86400
        } else { to_str <- paste0(round(mi_reg.s/86400, 2), ' días') } } }
    to_str <- paste0("Tiempo acumulado: ", trimws(to_str, "left"))
    if(b_print & b_print_una_linea)
      print.s <- paste0(print.s, "  -  ", to_str)
    else
      print_acc.s <- to_str
  }
  if(nchar(print.s) != 0) print(print.s)
  if(nchar(print_acc.s) != 0) print(print_acc.s)
  return(mi_tiempo_tot)
}

# ##################################################
# ## Funciones:
# ##################################################

procesar_campos_tweets <- function(tweets.list) # Añadimos campos nuevos (si hay) a partir de tweets.list
{
  # Campos a utilizar a priori (esta variable es la que se devuelve):
  CamposTweet <- read.csv2(file = 'CamposTweet.csv', stringsAsFactors = F, blank.lines.skip = TRUE)
  CamposTweet <- unique(as.vector(t(CamposTweet))) # Convertimos a vector unique
  # Cargamos 'CamposTweet_All.csv':
  CamposTweet.All <- read.csv2(file = 'CamposTweet_All.csv', stringsAsFactors = F, blank.lines.skip = TRUE)
  CamposTweet.All <- unique(as.vector(t(CamposTweet.All))) # Convertimos a vector unique
  # Creamos lista de campos a partir de la lista de tweets;
  Campos.tweets.list <- unique(names(unlist(tweets.list)))
  
  CamposNuevos <- Campos.tweets.list[!(Campos.tweets.list %in% CamposTweet.All)]
  if(length(CamposNuevos) == 0)
    return(CamposTweet); # Ok. No hay campos nuevos. No hacemos nada.
  
  print(paste0('Se han encontrado ', length(CamposNuevos), ' nuevo(s) campo(s) de tweets. Actualizamos fichero <CamposTweet_All.csv>...'))
  stop(paste0('Se han encontrado ', length(CamposNuevos), ' nuevo(s) campo(s) de tweets. Actualizamos fichero <CamposTweet_All.csv>...'))
  
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
                          , CamposTweet.All[grep(pattern = "entities.media", x = CamposTweet.All)] # Fotos/Videos
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
                          , CamposTweet.All[grep(pattern = "contributors.", x = CamposTweet.All)] # Tabla aparte
                          , CamposTweet.All[grep(pattern = "entities.", x = CamposTweet.All)] # Tabla aparte
                          , CamposTweet.All[grep(pattern = "place.attributes.", x = CamposTweet.All)] # Tabla aparte
                          , CamposTweet.All[grep(pattern = "annotations.", x = CamposTweet.All)] # Tabla aparte (Future/beta)
                          , CamposTweet.All[grep(pattern = "scopes.", x = CamposTweet.All)] # Tabla aparte (used by Twitter's Promoted Products)
                          , CamposTweet.All[grep(pattern = "geo.", x = CamposTweet.All)] # deprecated (use coordinates instead)
                          , CamposTweet.All[grep(pattern = ".following", x = CamposTweet.All)] # deprecated
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
  }
  else
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

procesar_tweets_json <- function(json_tweets_filename = "Prueba_tweets2", json_full_path = 'C:/Users/Jose/Documents/R/', mi_tiempo_total = NULL)
{
  if(endsWith(json_tweets_filename, '.json')) # Quitamos extensión ".json"
    json_tweets_filename <- substring(json_tweets_filename, 1, nchar(json_tweets_filename) - 5)
  
  stopifnot(file.exists(paste0(json_full_path, json_tweets_filename, '.json')))
  # ## Cargamos list "tweets.list" con readTweets(): # Con esto se pueden obtener algunas propiedades más de los tweets y es más rápido que parseTweets(), pero devuelve una lista.
  mi_tiempo <- system.time({
    tweets.list <- readTweets(tweets = paste0(json_full_path, json_tweets_filename, '.json'), verbose = TRUE)
  })
  mi_tiempo_total <- add_tiempo(mi_tiempo, "readTweets()", mi_tiempo_tot = mi_tiempo_total) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
  
  # ## procesar_campos_tweets():
  mi_tiempo <- system.time({
    CamposTweet <- procesar_campos_tweets(tweets.list = tweets.list)
  })
  mi_tiempo_total <- add_tiempo(mi_tiempo, "procesar_campos_tweets()", mi_tiempo_tot = mi_tiempo_total) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
  
  # ## procesa_lista_tweets():
  mi_tiempo <- system.time({
    tweets.df <- procesa_lista_tweets(tweets.list = tweets.list, CamposTweet = CamposTweet)
  })
  mi_tiempo_total <- add_tiempo(mi_tiempo, "procesa_lista_tweets()", mi_tiempo_tot = mi_tiempo_total) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
  
  # ## procesa_contenido_campos():
  mi_tiempo <- system.time({
    tweets.df <- procesa_contenido_campos(tweets.df)
  })
  mi_tiempo_total <- add_tiempo(mi_tiempo, "procesa_contenido_campos()", mi_tiempo_tot = mi_tiempo_total) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
  
  # ## guardar_df_full():
  mi_tiempo <- system.time({
    write.csv2(x = tweets.df, file = paste0('CSV/', json_tweets_filename, '_df_full.csv'), row.names = FALSE)
  })
  mi_tiempo_total <- add_tiempo(mi_tiempo, "guardar_df_full()", mi_tiempo_tot = mi_tiempo_total) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
  
  # # ## CANCELAMOS ESTA PARTE PORQUE parseTweets() DA ERRORES Y NO GANAMOS NADA.
  # # ## Cargamos data.frame "tweets.df.simplif" con parseTweets(): # parse the json file and save to a data frame (Simplify = FALSE ensures that we include lat/lon information in that data frame)
  # mi_tiempo <- system.time({
  #   tweets.df.simplif <- parseTweets(paste0(json_full_path, json_tweets_filename, '.json'), simplify = FALSE, verbose = TRUE)
  # })
  # mi_tiempo_total <- add_tiempo(mi_tiempo, "parseTweets()", mi_tiempo_tot = mi_tiempo_total) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
  #
  # # ## guardar_df_simpl():
  # mi_tiempo <- system.time({
  #   write.csv2(x = tweets.df.simplif, file = paste0('CSV/', json_tweets_filename, '_df_simpl.csv'), row.names = FALSE)
  #   # ## guardar_df_simpl_sin_RT():
  #   tweets.df.simplif_sin_RT <- tweets.df.simplif[substr(tweets.df.simplif$text, 1, 3) != 'RT ',]
  #   write.csv2(x = tweets.df.simplif_sin_RT, file = paste0('CSV/', json_tweets_filename, '_df_simpl_sin_RT.csv'), row.names = FALSE)
  # })
  # mi_tiempo_total <- add_tiempo(mi_tiempo, "guardar_df_simpl()", mi_tiempo_tot = mi_tiempo_total) # Para reiniciar: if(exists("mi_tiempo_total")) rm(mi_tiempo_total)
  # 
  # # ## CANCELAMOS ESTA PARTE PORQUE parseTweets() DA ERRORES Y NO GANAMOS NADA.
  
  # Devolvemos mi_tiempo_total:
  return(mi_tiempo_total)
}

procesar_tweets_json.Path <- function(i_sProyecto.s = 'Prueba_tweets',
                                      i_sPath = 'C:/Users/Jose/Documents/R/',
                                      i_sPathOut = paste0(getwd(), '/CSV'),
                                      i_bProcesarTodo = FALSE, i_bResetTime = TRUE, i_bPrint = TRUE,
                                      mi_tiempo_total = NULL)
{
  systime_ini_fun <- proc.time()
  fecha_ini_fun <- Sys.time()
  # Inicializamos variable para guardar los tiempos (si no existe):
  if(!exists('mi_tiempo_total'))  mi_tiempo_total <- data.frame(proc = character(), segundos = numeric(), to_str = character(), stringsAsFactors = FALSE)
  if(is.null('mi_tiempo_total'))  mi_tiempo_total <- data.frame(proc = character(), segundos = numeric(), to_str = character(), stringsAsFactors = FALSE)
  if(i_bResetTime)                mi_tiempo_total <- data.frame(proc = character(), segundos = numeric(), to_str = character(), stringsAsFactors = FALSE)
  # Añadimos más campos a mi_tiempo_total:
  mi_tiempo_total <- cbind(mi_tiempo_total, 'fich' = character(), 'fecha' = numeric(), 'ret.val' = numeric(), 'num.errs' = numeric())
  if(!('fich' %in% colnames(mi_tiempo_total)))
  { if(nrow(mi_tiempo_total) > 0) mi_tiempo_total[, 'fich'] <- ''  else  mi_tiempo_total <- cbind(mi_tiempo_total, 'fich' = character()) }
  if(!('fecha' %in% colnames(mi_tiempo_total)))
  { if(nrow(mi_tiempo_total) > 0) mi_tiempo_total[, 'fecha'] <- fecha_ini_fun  else  mi_tiempo_total <- cbind(mi_tiempo_total, 'fecha' = numeric()) }
  if(!('ret.val' %in% colnames(mi_tiempo_total)))
  { if(nrow(mi_tiempo_total) > 0) mi_tiempo_total[, 'ret.val'] <- 0  else  mi_tiempo_total <- cbind(mi_tiempo_total, 'ret.val' = numeric()) }
  if(!('num.errs' %in% colnames(mi_tiempo_total)))
  { if(nrow(mi_tiempo_total) > 0) mi_tiempo_total[, 'num.errs'] <- 0  else  mi_tiempo_total <- cbind(mi_tiempo_total, 'num.errs' = numeric()) }
  # Inicializamos parámetros:
  sPath <- file.path(i_sPath)
  if(i_sPathOut == FALSE)  sPathOut <- sPath  else  sPathOut <- file.path(i_sPathOut)
  # Ficheros json: "Proyecto"[_-]AAAA[_-]MM[_-]DD[_-]HH[_-]MM * .json
  regpat <- paste0('^', i_sProyecto.s, '[_-]?[0-9]{4}[_-][0-9]{1,2}[_-][0-9]{1,2}[_-][0-9]{1,2}[_-][0-9]{1,2}[_-][0-9]{1,2}')
  mis_ficheros <- list.files(path = sPath, ignore.case = TRUE,
                             pattern = paste0(regpat, '\\.json$'))
  nTot <- length(mis_ficheros)
  if(nTot > 0 & !i_bProcesarTodo)
  {
    # Ficheros ya procesados (csv) antes de empezar:
    mis_ficheros_ya_procesados <- list.files(path = sPathOut, ignore.case = TRUE, pattern = paste0('^', i_sProyecto.s, '.*\\.csv$'))
    # Ficheros json NO procesados todavía:
    mis_ficheros <- mis_ficheros[!(str_replace(mis_ficheros, '.json', '_df_full.csv') %in% mis_ficheros_ya_procesados)]
    # mis_ficheros <- mis_ficheros[!(str_replace(mis_ficheros, '.json', '_df_simpl.csv') %in% mis_ficheros_ya_procesados)]
    nTot <- length(mis_ficheros)
  }
  nOks <- 0
  nErrs <- 0
  if(nTot > 0)
  {
    # Dejamos el más reciente para el final, por si falla (puede estar siendo escrito todavía):
    mis_ficheros <- sort(mis_ficheros)
    
    # ==================================================================================
    # Paralelizamos:
    resAll.df <- foreach(f = mis_ficheros, .inorder=FALSE, .combine = rbind, .packages=c('stringr','streamR'),
                      .export = c("procesar_tweets_json", "add_tiempo", "procesar_campos_tweets", "procesa_lista_tweets", "procesa_contenido_campos")
    ) %dopar%
    {
      systime_ini <- proc.time()
      fecha_ini <- Sys.time()
      print(paste0(' - - Procesando fichero \'', f, '\'...'))
      mi_tiempo_total.fich <- try(
        procesar_tweets_json(json_tweets_filename = f, json_full_path = i_sPath)
        , silent = FALSE)
      if(inherits(mi_tiempo_total.fich, "try-error"))
      {
        ret_val <- 99 # ERROR
        res.df <- data.frame(proc = 'procesar_tweets_json()', segundos = as.double((proc.time() - systime_ini)['elapsed']), to_str = geterrmessage(), stringsAsFactors = FALSE)
      } else {
        ret_val <- 0 # 0 = OK
        res.df <- mi_tiempo_total.fich
      }
      # Añadimos más campos a res.df:
      res.df$fich <- f
      res.df$fecha <- fecha_ini
      res.df$ret.val <- 0
      res.df$num.errs <- 0
      res.df[nrow(res.df), ]$ret.val <- ret_val # Si hay error, sólo lo ponemos en la últ. fila
      res.df[nrow(res.df), ]$num.errs <- sum(res.df$num.errs != 0) # Si hay error, sólo lo ponemos en la últ. fila
      return(res.df) # ret_val de la función "foreach() %do%" (o foreach( %dopar%))
    }
    # ==================================================================================
    # Estadísticas finales:
    nOks <- sum(resAll.df$ret.val == 0)
    nErrs <- sum(resAll.df$num.errs != 0)
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
      print(paste0("Tiempo Total acumulado: ", round(sum(as.double(resAll.df$segundos)) / 60, 3), " minutos. [Ficheros Ok: ", nOks ,"/", nTot, " (", round(100*nOks/nTot,2) , "%)]"))
      print(paste0("Tiempo Total REAL: ", round(as.double(((proc.time() - systime_ini_fun)['elapsed'])) / 60, 3), " minutos."))
    }
  } else
  {
    resAll.df <- data.frame(proc = 'procesar_tweets_json()', segundos = as.double((proc.time() - systime_ini_fun)['elapsed']), to_str = 'Ok. No hay ficheros.', stringsAsFactors = FALSE)
    # Añadimos más campos a res.df:
    resAll.df$fich <- i_sProyecto.s
    resAll.df$fecha <- fecha_ini_fun
    resAll.df$ret.val <- 0
    resAll.df$num.errs <- 0
    print('Ok. No hay ficheros.')
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

# ##################################################
# ## Inicio:
# ##################################################
########################################################################################################
Proyecto <- "Procesar Tweets JSON to CSV"
########################################################################################################
print(paste0(Sys.time(), ' - ', 'Proyecto = ', Proyecto))

# NOTA: Dejamos un Core de la CPU "libre" para no "quemar" la máquina:
cl <- makeCluster(detectCores() - 1, type='PSOCK') # library(doParallel) [turn parallel processing on]
registerDoParallel(cl) # library(doParallel) [turn parallel processing on]

mi_tiempo_total <- add_tiempo(NULL)
mi_tiempo_total <- procesar_tweets_json.Path("Prueba_tweets", i_bProcesarTodo = T, mi_tiempo_total = mi_tiempo_total)

if(mi_tiempo_total[nrow(mi_tiempo_total), ]$num.errs != 0)
  print(mi_tiempo_total[nrow(mi_tiempo_total), ])
# Leemos ficheros csv de tweets (ya procesados):
# mi_df <- read.csv2('Prueba_tweets2016_10_18_22_36_13_df_full.csv', stringsAsFactors = F, blank.lines.skip = TRUE)

# cleanup:
try(registerDoSEQ(), silent = TRUE) # library(doParallel) [turn parallel processing off and run sequentially again]
try(stopImplicitCluster(), silent = TRUE)
try(stopCluster(cl), silent = TRUE)
