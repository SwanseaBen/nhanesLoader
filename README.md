# nhanesLoader

<div>
<h3 align="center">NHANES Webscraper</h3>

  <p align="center">
    This is some simple Python code to download the open NHANES datasets to simplify the task of researchers. As Python is not a language I am using daily, there may be some issues with the code. 
    <br />
  </p>
</div>

### Built With

* Python
* <a href="https://pypi.org/project/beautifulsoup4/"> Beautiful Soup Library</a>

## Getting Started

### Prerequisites

Python programming knowledge

### Installation

Install Python and Beautiful Soup first

## Usage

One can scrap data by adding into a python code:

```
import nhanes.loader as nl
from nhanes.variables import Tests

test_list = ["THYROD", "CBC"]
current_dir = "C:\tmp\\"
csv_file = 'C:/output.csv'
nl.nhanes_merger_numpy(current_dir + "Nhanes\\", test_list, destination=csv_file, all=True) # Scrape and creates CSV
df = nl.load_csv(csv_file, min_age=18, max_age=25) # Load the created CSV file into a dataframe
```

## Contributing

Code is given as it is, with no assumption this will work. Feel free to contribute!

>

## License

Distributed under the MIT License. 
