install.packages("shiny")
library(shiny)
ui <- fluidPage(
  selectInput('dataType', 'choose the data type',
              c('ozone' = "Ozone", 'solar' = "Solar.R", 'wind' = "Wind", 'temperature' = "Temp")),
  selectInput('graphType', 'choose the graph type', c('line' = "l", 'bar' = "h")),
  plotOutput("plot")
)

server <- function(input, output) {
  output$plot <- renderPlot({
    plot(remove_outlier(na.omit(frame[[input$dataType]])), ylab = input$dataType, type = input$graphType)
  })
}

shinyApp(ui = ui, server = server)

