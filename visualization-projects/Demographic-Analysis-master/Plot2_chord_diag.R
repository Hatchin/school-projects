library(plotly)
library(dplyr)
library(chorddiag)

# set dir
setwd("~/Desktop/Spring_mod2/DV/project/yuyun2.github.io/data/Plot2_ChordDiagram_MigrationState/") # set dir

# read data 
MyData <- read.csv(file="migration_total_sorted_by_region.csv", sep=",", stringsAsFactors = FALSE) # read data 

# define col names 
states <- c('Illinois', 'Indiana', 'Iowa', 'Kansas', 'Michigan',
            'Minnesota', 'Missouri', 'Nebraska', 'North Dakota', 'Ohio',
            'South Dakota', 'Wisconsin', 'Connecticut', 'Maine', 'Massachusetts',
            'New Hampshire', 'New Jersey', 'New York', 'Pennsylvania',
            'Rhode Island', 'Vermont', 'Alabama', 'Arkansas', 'Delaware',
            'District of Columbia ', 'Florida', 'Georgia', 'Kentucky', 'Louisiana',
            'Maryland', 'Mississippi', 'North Carolina', 'Oklahoma',
            'South Carolina', 'Tennessee', 'Texas', 'Virginia', 'West Virginia',
            'Alaska', 'Arizona', 'California', 'Colorado', 'Hawaii', 'Idaho',
            'Montana', 'Nevada', 'New Mexico', 'Oregon', 'Utah', 'Washington',
            'Wyoming'
            )

# process data 
x <- MyData[, 1:51] # subset data
colnames(x) <- states # assign col names 
data <- as.matrix(x)  # convert to matrix 
row.names(data) <- states # assign row names 

# add color
group_colors <- c(
  '#8dd3c7', '#8dd3c7', '#8dd3c7', '#8dd3c7', '#8dd3c7', '#8dd3c7',
  '#8dd3c7', '#8dd3c7', '#8dd3c7', '#8dd3c7', '#8dd3c7', '#8dd3c7',
  '#ffffb3', '#ffffb3', '#ffffb3', '#ffffb3', '#ffffb3', '#ffffb3',
  '#ffffb3', '#ffffb3', '#ffffb3', '#bebada', '#bebada', '#bebada',
  '#bebada', '#bebada', '#bebada', '#bebada', '#bebada', '#bebada',
  '#bebada', '#bebada', '#bebada', '#bebada', '#bebada', '#bebada',
  '#bebada', '#bebada', '#fb8072', '#fb8072', '#fb8072', '#fb8072',
  '#fb8072', '#fb8072', '#fb8072', '#fb8072', '#fb8072', '#fb8072',
  '#fb8072', '#fb8072', '#fb8072'
)

# plot chord diagram
chorddiag(data, groupColors = group_colors, 
          type = "directional", showTicks = F, 
          groupnameFontsize = 14, groupnamePadding = 10,
          margin = 90, chordedgeColor = '#d6d6d6', showGroupnames=TRUE)


