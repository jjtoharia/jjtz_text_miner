options(echo = FALSE) # ECHO OFF
print('#######################################################################################################')
print('# jjtz_text_miner')
print('#######################################################################################################')
### Inicialización (setwd() y rm() y packages):

# setwd(getwd())
try(setwd('C:/Users/jtoharia/Dropbox/AFI_JOSE/Proyecto Final/TextMining'), silent=TRUE)
try(setwd('C:/Personal/Dropbox/AFI_JOSE/Proyecto Final/TextMining'), silent=TRUE)
rm(list = ls()) # Borra todos los elementos del entorno de R.

# install.packages("ROAuth")
library(ROAuth)
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

# # Para debug:
# options(echo = TRUE)

# ##################################################
# ## Funciones:
# ##################################################
check_Go <- function(Proyecto.s)
{
  if(!file.exists(paste0(Proyecto.s, '.GO')))
  {
    try(write("Buh!", file = paste0(Proyecto.s, '.stopped')), silent = TRUE)
    stop(paste0('Fichero <', Proyecto.s, '.GO', '> NO encontrado. Detenemos el proceso...'))
  }
}

finaliza <- function()
{
  # Finalmente, contamos los tweets realmente guardados en el json:
  nTweets   <<- length(readTweets(tweets = paste0('C:/Users/Jose/Documents/R/', json_tweets_filename, '.json'), verbose = TRUE))
  print(paste0(Sys.time(), ' - ', nTweets, ' tweets guardados.'))

  # Guardamos las estadísticas (fecha, json_filename, trackWords, numSeconds, numTweetsDwnld, numTweets)
  # en el CSV "Proyecto.CSV":
  local.Proyecto.s <- Proyecto.s
  while(file.exists(paste0(local.Proyecto.s, '..CSV')))
  {
    local.Proyecto.s <- paste0(local.Proyecto.s, '.') # Buscamos el último si hubo algún error en algún momento
  }
  b_CsvExiste <- file.exists(paste0(local.Proyecto.s, '.CSV'))

  nErrCount <- 0
  while(inherits(
    try(
      write.table(
        x = data.frame(fecha           = Sys.time(),
                       json_filename   = json_tweets_filename,
                       trackWords      = paste0(trackWords, collapse = ','),
                       numSeconds      = nSecs,
                       numTweetsDwnld  = nTweetsDownloaded,
                       numTweets       = nTweets),
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

  nSecsAcc             <<- nSecsAcc + nSecs
  nTweetsDownloadedAcc <<- nTweetsDownloadedAcc + nTweetsDownloaded
  nTweetsAcc           <<- nTweetsAcc + nTweets
  if(nSecsAcc > nSecs)
  {
    print(paste0(Sys.time(), ' - ', nSecsAcc, ' segundos (acumulados).'))
    print(paste0(Sys.time(), ' - ', nTweetsDownloadedAcc, ' tweets descargados (acumulados).'))
    print(paste0(Sys.time(), ' - ', nTweetsAcc, ' tweets guardados (acumulados).'))
  }
}

# ##################################################
# ## Inicio:
# ##################################################
########################################################################################################
Proyecto <- "Capturar Tweets"
timeout_seconds <- 600 # Keep connection alive for N seconds
tot_conex <- 24 # Total number of connections (files created)
trackWords <- c("investidura", "abstención", "partido socialista", "PSOE",
                "partido popular", "PP", "Podemos", "Ciudadanos", "CS",
                "Mariano Rajoy", "Pedro Sánchez", "Pablo Iglesias", "Albert Rivera")
########################################################################################################
print(paste0(Sys.time(), ' - ', 'Proyecto = ', Proyecto))
print(paste0(Sys.time(), ' - ', 'timeout_seconds = ', timeout_seconds))
print(paste0(Sys.time(), ' - ', 'trackWords = ', paste0(trackWords, collapse = ',')))

Proyecto.s <- str_replace_all(Proyecto, "\\(|\\)| |:", "_") # Quitamos espacios, paréntesis, etc.

check_Go(Proyecto.s) # Verificamos fichero GO para salir con Ok o con error 9

# Cargamos el objeto my_oauth: (creado y guardado con Crear_OAuth.R)
numReintentos <- 1
load("jjtz_text_miner_oauth.Rdata")
load("jjtz_text_miner2_oauth.Rdata")

# Inicializamos variables:
fecha <- Sys.time()
fecha.s <- str_replace(str_replace_all(fecha, pattern = "-|:", replacement = "_"), pattern = " ", replacement = "-")
json_tweets_filename <- paste0(Proyecto.s, "_", fecha.s)
nTweetsAcc <- 0
nSecsAcc <- 0
nTweetsDownloadedAcc <- 0
# ##################################################
# ## Hacemos un bucle de tot_conex conexiones
# ## cada una hasta un máx. de timeout_seconds:
# ## NOTA: Se crea un fichero nuevo a cada vez
# ##################################################
tryCatch(
  for(num_conex in 1:tot_conex) # 600 * 24 (14400 segs == 4 horas)
  {
    nTweets <<- 0
    nSecs <<- 0
    nTweetsDownloaded <<- 0
    fecha <- Sys.time()
    fecha.s <- str_replace(str_replace_all(fecha, pattern = "-|:", replacement = "_"), pattern = " ", replacement = "-")
    json_tweets_filename <- paste0(Proyecto.s, "_", fecha.s)
    
    # if(file.exists(paste0(json_tweets_filename, '.json')))  file.remove(paste0(json_tweets_filename, '.json'))
    
    p <- capture.output( # Capturamos los mensajes que devuelve esta función
      filterStream(file.name = paste0('C:/Users/Jose/Documents/R/', json_tweets_filename, '.json'), # Save tweets in a json file (APPEND!!!)
                 # Collect tweets mentioning either Affordable Care Act, ACA, or Obamacare:
                 track = trackWords,
                 language = "es",
                 # # latitude/longitude pairs providing southwest and northeast corners of the bounding box:
                 # location = c(-119, 33, -117, 35),
                 timeout = timeout_seconds, # Keep connection alive for N seconds
                 oauth = my_oauth, # Use my_oauth file as the OAuth credentials
                 verbose = TRUE)
      , type = c("message")) # Capturamos los mensajes que devuelve esta función

    print(p) # Mostramos esos mensajes (al capturarlos ya no aparecen en consola)
    # Procesamos el mensaje para contar los tweets descargados (que son más que los guardados)
    ult <- p[endsWith(p, 'tweets downloaded.')]
    ult <- sub(x = ult,
               pattern = '(Connection to Twitter stream was closed after )(.*)( seconds with up to )(.*)( tweets downloaded.)',
               replacement = '\\2 \\4')
    if(length(ult) != 0)
    {
      nSecs <<- as.integer(sub(x = ult, pattern = '(.*)( )(.*)', replacement = '\\1'))
      nTweetsDownloaded <<- as.integer(sub(x = ult, pattern = '(.*)( )(.*)', replacement = '\\3'))
      print(paste0(Sys.time(), ' - ', nSecs, ' segundos.'))
      print(paste0(Sys.time(), ' - ', nTweetsDownloaded, ' tweets descargados.'))
      if(nSecs < timeout_seconds - 10)
      {
        numReintentos <- numReintentos + 1
        print(paste0(Sys.time(), ' - Cambio de conexión (y van ', numReintentos, ' cambios de conexión).'))
        if(numReintentos %% 2 == 0) load("jjtz_text_miner_oauth.Rdata") else load("jjtz_text_miner2_oauth.Rdata")
      }
    }
    
    if(!file.exists(paste0(Proyecto.s, '.GO')))  { break }
    
    if(num_conex != tot_conex) # ¡La última ya se hace en el finally!
      finaliza() # Guardamos estadísticas del fichero
    
  },
  finally = finaliza() # En caso de error, tratamos de guardar lo que llevábamos hasta ahora antes de salir
)

check_Go(Proyecto.s) # Verificamos fichero GO para salir con Ok o con error 9

options(echo = FALSE) # ECHO OFF
# # Con esto se pueden obtener algunas propiedades más de los tweets y es más rápido que parseTweets(). Pero devuelve una lista.
# tweets.list <- readTweets(tweets = paste0(json_tweets_filename, '.json'), verbose = TRUE)
# p<-as.data.frame(t(as.matrix(tweets.list)))
# write.csv(x = tweets.list, file = paste0(json_tweets_filename, '_lst.csv'))

# # parse the json file and save to a data frame called tweets.df:
# # Simplify = FALSE ensures that we include lat/lon information in that data frame:
# tweets.df <- parseTweets(paste0(json_tweets_filename, '.json'), simplify = FALSE, verbose = TRUE)
# write.csv(x = tweets.df, file = paste0(json_tweets_filename, '_df.csv'))

# # Para debug:
# options(echo = TRUE)
