####################################################
#### Funciones útiles:
####################################################
#----------------------------------------------------------------------------
# ### check_Go(): stop() si no existe el fichero Proyecto.s & '.GO'
# ###   - Proyecto.s (string): Nombre del proyecto/fichero (sin ".GO")
check_Go <- function(Proyecto.s)
{
  if(!file.exists(paste0(Proyecto.s, '.GO')))
  {
    try(write("Buh!", file = paste0(Proyecto.s, '.stopped')), silent = TRUE)
    stop(paste0('Fichero <', Proyecto.s, '.GO', '> NO encontrado. Detenemos el proceso...'))
  }
}
#----------------------------------------------------------------------------
# ### init_tiempo(): Inicializar campos de la variable mi_tiempo_total
# ### NOTA: Esta función no crea ningún registro.
# ###       Sólo añade campos, si hace falta, y devuelve df vacío, si hace falta.
# ###   - mi_tiempo_tot (data.frame): puede ser mi_tiempo_total o NULL para crearla nueva
# ###   - i_bResetTime (boolean): TRUE para vaciar mi_tiempo_total
init_tiempo <- function(mi_tiempo_tot = NULL, i_bResetTime = FALSE)
{
  # Usamos la variable global "mi_tiempo_total", si existe:
  if(is.null(mi_tiempo_tot) & exists('mi_tiempo_total'))
  {
    mi_tiempo_tot <- mi_tiempo_total
  }
  bTmp <- i_bResetTime
  if(!bTmp)    bTmp <- is.null(mi_tiempo_tot)
  if(!bTmp)    bTmp <- (nrow(mi_tiempo_tot) == 0)
  if(bTmp) # if(i_bResetTime | is.null(mi_tiempo_tot) | nrow(mi_tiempo_tot) == 0)
  {
    # Creamos mi_tiempo_tot, vacío, con más campos:
    mi_tiempo_tot <- data.frame(proc = character(), segundos = numeric(), to_str = character(),
                                fich = character(), fecha = numeric(),
                                ret.val = numeric(), num.errs = numeric(),
                                stringsAsFactors = FALSE)
    # Cambiamos la clase del campo fecha (no sé crear ese objeto vacío):
    mi_tiempo_tot$fecha <- as.POSIXct(mi_tiempo_tot$fecha, origin = "1970-01-01 00:00.00 UTC")
  } else
  {
    # Añadimos más campos, si no están, a mi_tiempo_tot:
    if(!('fich' %in% colnames(mi_tiempo_tot)))
      mi_tiempo_tot[, 'fich'] <- ''
    if(!('fecha' %in% colnames(mi_tiempo_tot)))
      mi_tiempo_tot[, 'fecha'] <- Sys.time()
    if(!('ret.val' %in% colnames(mi_tiempo_tot)))
      mi_tiempo_tot[, 'ret.val'] <- 0
    if(!('num.errs' %in% colnames(mi_tiempo_tot)))
      mi_tiempo_tot[, 'num.errs'] <- 0
  }
  if(exists('mi_tiempo_total'))
  {
    mi_tiempo_total <- mi_tiempo_tot
  }
  # Y devolvemos data.frame:
  return(mi_tiempo_tot)
}
#----------------------------------------------------------------------------
# ### add_tiempo(): Devuelve data.frame con los tiempos de cada llamada:
# ###               data.frame( proc = character(), segundos = numeric(), to_str = character(), stringsAsFactors = FALSE)
add_tiempo <- function(mi_tiempo, proc_txt = "", mi_tiempo_tot = NULL,
                       ret_val = 0, num_errs = 0, fich = "", fecha = Sys.time(),
                       b_print = FALSE, b_print_acum = FALSE, b_print_una_linea = TRUE)
{
  # Creamos data.frame "mi_tiempo_tot", vacío, si no se ha creado todavía:
  if(is.null(mi_tiempo_tot))
    mi_tiempo_tot <- init_tiempo()
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
  # Insertamos nuevo registro:
  mi_tiempo_tot[nrow(mi_tiempo_tot) + 1, 'proc'] <- proc_txt
  # Actualizamos último registro:
  mi_tiempo_tot[nrow(mi_tiempo_tot), 'segundos'] <- as.numeric(mi_reg.s)
  mi_tiempo_tot[nrow(mi_tiempo_tot), 'to_str']   <- to_str
  mi_tiempo_tot[nrow(mi_tiempo_tot), 'ret.val']  <- ret_val
  mi_tiempo_tot[nrow(mi_tiempo_tot), 'num.errs'] <- num_errs
  mi_tiempo_tot[nrow(mi_tiempo_tot), 'fich'] <- fich
  mi_tiempo_tot[nrow(mi_tiempo_tot), 'fecha'] <- fecha
  
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
#----------------------------------------------------------------------------
# ### relevancia(): Relevancia de una variable categórica respecto al target (Test Chi2)
relevancia <- function(VariableTarget, VariableCategorica)
{
  levels <- levels(VariableCategorica)
  colors <- c()
  for (i in 1:(length(levels)))
  {
    TABLA <- table(VariableTarget,VariableCategorica==levels[i])
    chi <- chisq.test(TABLA)
    if(chi$p.value < 0.05)
    {
      colors <- c(colors, "green")
    }else
    {
      colors <- c(colors, "gray")
    }
  }
  TABLA <- table(VariableTarget, VariableCategorica)
  plot <- barplot(100 * TABLA[2,] / (TABLA[1,] + TABLA[2,]), ylim = c(0,100), col = colors, cex.names = 0.6)
  text(x = plot, y = 5 + 100 * TABLA[2,] / (TABLA[1,] + TABLA[2,]), labels = paste(round(100 * TABLA[2,] / (TABLA[1,] + TABLA[2,]), 2), "%", sep = ""))
  abline(h = 100 * prior, col = "red")
  return(TABLA)
}
#----------------------------------------------------------------------------
# ### woe_iv(): Análisis de la capacidad predictiva de las variables
# ###           Weigth of Evidence (WOE) and  Information Value (IV)
woe_iv <- function(VariableTarget,VariableCategorica)
{
  Tabla_WOE <- table(VariableCategorica, VariableTarget)
  DF_WOE <- data.frame(FRACASOS = Tabla_WOE[,1], EXITOS = Tabla_WOE[,2])
  DF_WOE$EXITOS_PORC <- DF_WOE$EXITOS / sum(DF_WOE$EXITOS)
  DF_WOE$FRACASOS_PORC <- DF_WOE$FRACASOS / sum(DF_WOE$FRACASOS)
  DF_WOE$WOE <- log(DF_WOE$EXITOS_PORC / DF_WOE$FRACASOS_PORC)
  DF_WOE$IV <- (DF_WOE$EXITOS_PORC - DF_WOE$FRACASOS_PORC) * DF_WOE$WOE
  DF_WOE
  return(DF_WOE)
}
#----------------------------------------------------------------------------
