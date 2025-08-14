# =========================================================
# ak_downscale_combined.R
# Alaska GHG downscaling (Industrial & Transport) + Shiny UI
# - Reads EPA SIT workbook (Economic Sectors)
# - Builds AK year × sector totals
# - Downscales Industrial (employment) and Transport (Population/Vehicles/Passenger)
# - Exports tidy + 0–1 scaled CSVs (with 4 fixed classes)
# - OR launch an interactive Shiny app to tune weights and download outputs
# =========================================================

suppressPackageStartupMessages({
  library(readxl); library(dplyr); library(tidyr); library(janitor)
  library(stringr); library(readr); library(optparse)
  library(shiny); library(DT); library(plotly)
})

# ---------- Helpers ----------
get_year_cols <- function(nms) nms[grepl("^[Yy]\\d{4}$", nms)]

read_state_econ <- function(path, sheet, state="AK"){
  df <- read_excel(path, sheet = sheet) |> clean_names()
  yrs <- get_year_cols(names(df))
  if (length(yrs) == 0) stop("No year columns found (expected Y1990..Y2022).")
  if (!"geo_ref" %in% names(df)) stop("Missing 'geo_ref' in sheet '", sheet, "'.")
  df |>
    filter(geo_ref == state) |>
    pivot_longer(all_of(yrs), names_to = "year", values_to = "state_total") |>
    mutate(year = as.integer(sub("^[Yy]", "", year)))
}

detect_label <- function(u, pat){
  hit <- u[grepl(pat, u, ignore.case = TRUE)]
  if (length(hit) == 0) NA_character_ else hit[1]
}

norm_share <- function(x) x / sum(x, na.rm = TRUE)

make_classes4 <- function(x){
  cut(x,
      breaks = c(0, 0.25, 0.5, 0.75, 1.000001),
      labels = c("0–0.25","0.25–0.5","0.5–0.75","0.75–1.0"),
      include.lowest = TRUE, right = FALSE)
}

run_pipeline <- function(epa_xlsx, sheet_econ, state_code,
                         proxies_csv, latest_year, w_pop=0.2, w_veh=0.6, w_pas=0.2){
  
  message("Loading EPA workbook...")
  ak <- read_state_econ(epa_xlsx, sheet_econ, state_code)
  
  # keep only the 6 economic sectors (exclude LULUCF rows if present)
  ak6 <- ak |> filter(!grepl("lulucf", econ_sector, ignore.case = TRUE))
  
  # year × sector totals
  ak_ysec <- ak6 |>
    group_by(year, econ_sector) |>
    summarise(state_total = sum(state_total, na.rm = TRUE), .groups = "drop")
  
  IND_LABEL <- detect_label(unique(ak_ysec$econ_sector), "industrial")
  TRN_LABEL <- detect_label(unique(ak_ysec$econ_sector), "transport")
  if (is.na(IND_LABEL) || is.na(TRN_LABEL)) {
    stop("Could not detect 'Industrial' or 'Transportation' in econ_sector labels.")
  }
  
  message("Reading proxies: ", proxies_csv)
  prox <- read_csv(proxies_csv, show_col_types = FALSE) |> clean_names()
  req_cols <- c("county","industrial_emp","population","vehicles")
  if (!all(req_cols %in% names(prox))) {
    stop("Proxies CSV must include columns: county, industrial_emp, population, vehicles ",
         "(optional: passenger). Missing: ",
         paste(setdiff(req_cols, names(prox)), collapse = ", "))
  }
  if (!"passenger" %in% names(prox)) prox$passenger <- 0
  prox <- mutate(prox, passenger = ifelse(is.na(passenger), 0, passenger))
  
  # ---- Industrial: weights by industrial_emp ----
  ind_wts <- prox |>
    mutate(weight_ind = industrial_emp / sum(industrial_emp, na.rm = TRUE)) |>
    select(county, weight_ind)
  
  ind_state <- ak_ysec |> filter(econ_sector == IND_LABEL)
  
  alloc_ind <- ind_state |>
    mutate(key = 1) |>
    inner_join(ind_wts |> mutate(key = 1), by = "key") |>
    select(-key) |>
    group_by(year) |>
    mutate(weight_ind = weight_ind / sum(weight_ind, na.rm = TRUE)) |>
    ungroup() |>
    mutate(emi = state_total * weight_ind,
           sector = "Industrial") |>
    select(county, sector, year, emi)
  
  # ---- Transport: composite weights ----
  trn_wts <- prox |>
    transmute(
      county,
      p_pop = norm_share(population),
      p_veh = norm_share(vehicles),
      p_pas = norm_share(passenger),
      proxy_trn = w_pop*p_pop + w_veh*p_veh + w_pas*p_pas
    ) |>
    mutate(weight_trn = proxy_trn / sum(proxy_trn, na.rm = TRUE)) |>
    select(county, weight_trn)
  
  trn_state <- ak_ysec |> filter(econ_sector == TRN_LABEL)
  
  alloc_trn <- trn_state |>
    mutate(key = 1) |>
    inner_join(trn_wts |> mutate(key = 1), by = "key") |>
    select(-key) |>
    group_by(year) |>
    mutate(weight_trn = weight_trn / sum(weight_trn, na.rm = TRUE)) |>
    ungroup() |>
    mutate(emi = state_total * weight_trn,
           sector = "Transportation") |>
    select(county, sector, year, emi)
  
  # Combine + 0–1 scaling (per sector across all years)
  alloc_long <- bind_rows(alloc_ind, alloc_trn)
  
  alloc_scaled <- alloc_long |>
    group_by(sector) |>
    mutate(minv = min(emi, na.rm = TRUE),
           maxv = max(emi, na.rm = TRUE),
           scaled_0_1 = ifelse(maxv > minv, (emi - minv)/(maxv - minv), 0)) |>
    ungroup() |>
    select(-minv, -maxv) |>
    mutate(class_4 = make_classes4(scaled_0_1))
  
  # Exports
  write_csv(alloc_scaled, "AK_Downscaled_Industrial_Transport_Long.csv")
  
  wide_latest <- alloc_scaled |>
    filter(year == latest_year) |>
    select(county, sector, scaled_0_1) |>
    tidyr::pivot_wider(names_from = sector, values_from = scaled_0_1)
  
  write_csv(wide_latest, paste0("AK_Downscaled_Scaled0_1_Wide_", latest_year, ".csv"))
  
  message("Wrote:",
          "\n  - AK_Downscaled_Industrial_Transport_Long.csv",
          "\n  - AK_Downscaled_Scaled0_1_Wide_", latest_year, ".csv")
}

launch_shiny <- function(){
  ui <- fluidPage(
    titlePanel("Alaska Downscaling — Industrial & Transportation"),
    sidebarLayout(
      sidebarPanel(
        fileInput("xlsx", "EPA SIT workbook (.xlsx)", accept = ".xlsx"),
        textInput("sheet", "Sheet name", "Data by Economic Sectors"),
        textInput("state", "State code", "AK", width = "100px"),
        fileInput("prox", "Borough proxies CSV", accept = ".csv"),
        hr(),
        sliderInput("year", "Year", min = 1990, max = 2022, value = 2022, step = 1, sep = ""),
        h5("Transport weights (Population / Vehicles / Passenger)"),
        numericInput("w_pop", "Population", value = 0.20, min = 0, max = 1, step = 0.05, width = "120px"),
        numericInput("w_veh", "Vehicles",   value = 0.60, min = 0, max = 1, step = 0.05, width = "120px"),
        numericInput("w_pas", "Passenger",  value = 0.20, min = 0, max = 1, step = 0.05, width = "120px"),
        actionButton("renorm", "Renormalize weights to sum 1"),
        hr(),
        downloadButton("dl_long", "Download: Long Tidy CSV"),
        downloadButton("dl_wide", "Download: Wide (0–1) CSV")
      ),
      mainPanel(
        tabsetPanel(
          tabPanel("Allocations (table)", DTOutput("tbl")),
          tabPanel("Bar chart", plotlyOutput("bar", height = 440)),
          tabPanel("Notes",
                   tags$p("Industrial weighting uses borough industrial employment (CBP NAICS 31–33)."),
                   tags$p("Transportation weighting uses a composite of normalized Population, Vehicles, and Passenger."),
                   tags$p("Outputs include 0–1 scaling and fixed 4-class bins for QGIS mapping."))
        )
      )
    )
  )
  
  server <- function(input, output, session){
    
    observeEvent(input$renorm, {
      s <- sum(input$w_pop, input$w_veh, input$w_pas)
      if (s > 0) {
        updateNumericInput(session, "w_pop", value = input$w_pop / s)
        updateNumericInput(session, "w_veh", value = input$w_veh / s)
        updateNumericInput(session, "w_pas", value = input$w_pas / s)
      }
    })
    
    dfe <- reactive({
      req(input$xlsx)
      read_state_econ(input$xlsx$datapath, input$sheet, toupper(input$state))
    })
    
    prox <- reactive({
      req(input$prox)
      read_csv(input$prox$datapath, show_col_types = FALSE) |> clean_names()
    })
    
    lab_ind <- reactive({
      u <- unique(dfe()$econ_sector)
      detect_label(u, "industrial")
    })
    lab_trn <- reactive({
      u <- unique(dfe()$econ_sector)
      detect_label(u, "transport")
    })
    
    alloc <- reactive({
      df <- dfe(); pr <- prox()
      validate(need(all(c("county","industrial_emp","population","vehicles") %in% names(pr)),
                    "Proxies must have: county, industrial_emp, population, vehicles (passenger optional)."))
      if (!"passenger" %in% names(pr)) pr$passenger <- 0
      
      # state totals for selected year
      y <- input$year
      ind_state <- df |> filter(econ_sector == lab_ind(), year == y)
      trn_state <- df |> filter(econ_sector == lab_trn(), year == y)
      
      # industrial weights
      ind_wts <- pr |>
        mutate(weight_ind = industrial_emp / sum(industrial_emp, na.rm = TRUE)) |>
        select(county, weight_ind)
      
      alloc_ind <- ind_state |>
        mutate(key = 1) |>
        inner_join(ind_wts |> mutate(key = 1), by = "key") |>
        select(-key) |>
        mutate(emi = state_total * weight_ind, sector = "Industrial") |>
        select(county, sector, year, emi)
      
      # transport weights
      norm_share <- function(x) x / sum(x, na.rm = TRUE)
      w_pop <- input$w_pop; w_veh <- input$w_veh; w_pas <- input$w_pas
      trn_wts <- pr |>
        transmute(
          county,
          p_pop = norm_share(population),
          p_veh = norm_share(vehicles),
          p_pas = norm_share(ifelse(is.na(passenger), 0, passenger)),
          proxy_trn = w_pop*p_pop + w_veh*p_veh + w_pas*p_pas
        ) |>
        mutate(weight_trn = proxy_trn / sum(proxy_trn, na.rm = TRUE)) |>
        select(county, weight_trn)
      
      alloc_trn <- trn_state |>
        mutate(key = 1) |>
        inner_join(trn_wts |> mutate(key = 1), by = "key") |>
        select(-key) |>
        mutate(emi = state_total * weight_trn, sector = "Transportation") |>
        select(county, sector, year, emi)
      
      out <- bind_rows(alloc_ind, alloc_trn)
      
      # 0–1 scaling per sector
      out <- out |>
        group_by(sector) |>
        mutate(
          minv = min(emi, na.rm = TRUE),
          maxv = max(emi, na.rm = TRUE),
          scaled_0_1 = ifelse(maxv > minv, (emi - minv)/(maxv - minv), 0)
        ) |>
        ungroup() |>
        select(-minv, -maxv) |>
        mutate(class_4 = make_classes4(scaled_0_1))
      out
    })
    
    output$tbl <- renderDT({
      datatable(alloc() |> arrange(sector, desc(emi)),
                options = list(pageLength = 15, scrollX = TRUE))
    })
    
    output$bar <- renderPlotly({
      dt <- alloc() |> arrange(sector, desc(emi))
      plot_ly(dt, x = ~reorder(county, emi), y = ~emi, color = ~sector,
              type = "bar",
              hovertemplate = "%{x}<br>%{y:.3f} MMT CO₂e<extra>%{fullData.name}</extra>") |>
        layout(yaxis = list(title = "Emissions (MMT CO₂e)"),
               xaxis = list(title = "", tickangle = -35),
               barmode = "group", showlegend = TRUE)
    })
    
    output$dl_long <- downloadHandler(
      filename = function() paste0("AK_Downscaled_Industrial_Transport_", input$year, "_Long.csv"),
      content  = function(file) write_csv(alloc(), file)
    )
    
    output$dl_wide <- downloadHandler(
      filename = function() paste0("AK_Downscaled_Scaled0_1_Wide_", input$year, ".csv"),
      content  = function(file) {
        write_csv(alloc() |>
                    select(county, sector, scaled_0_1) |>
                    tidyr::pivot_wider(names_from = sector, values_from = scaled_0_1),
                  file)
      }
    )
  }
  
  shinyApp(ui, server)
}

# ---------- CLI entry ----------
option_list <- list(
  make_option("--mode", type="character", default="pipeline",
              help="Run mode: 'pipeline' or 'shiny' [default %default]"),
  make_option("--epa", type="character", default="AllStateGHGData90-22_v082924.xlsx",
              help="Path to EPA SIT workbook (.xlsx)"),
  make_option("--sheet", type="character", default="Data by Economic Sectors",
              help="Sheet name for Economic sectors"),
  make_option("--state", type="character", default="AK",
              help="USPS state code (e.g., AK)"),
  make_option("--proxies", type="character", default="ak_borough_proxies.csv",
              help="CSV with borough proxies: county,industrial_emp,population,vehicles[,passenger]"),
  make_option("--latest_year", type="integer", default=2022,
              help="Latest year for 'wide' export [default %default]"),
  make_option("--w_pop", type="double", default=0.20, help="Transport weight: Population"),
  make_option("--w_veh", type="double", default=0.60, help="Transport weight: Vehicles"),
  make_option("--w_pas", type="double", default=0.20, help="Transport weight: Passenger")
)

args <- parse_args(OptionParser(option_list=option_list))

if (tolower(args$mode) == "shiny") {
  launch_shiny()
} else {
  run_pipeline(epa_xlsx   = args$epa,
               sheet_econ = args$sheet,
               state_code = toupper(args$state),
               proxies_csv= args$proxies,
               latest_year= args$latest_year,
               w_pop = args$w_pop, w_veh = args$w_veh, w_pas = args$w_pas)
}
