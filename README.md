# OpenReviewSpider

## Archtecture

* [x] MetaData (Json)
* [x] DataBase (Sqlite)
* [x] Incremental Update
* [x] Query Co-Author by date (COI-assist)



```mermaid
flowchart TD
    RUN["OpenReviewSpider.run()"] --> GETCONF["get_all_conferences()"]
    GETCONF --> FORCONF{"for conf in conferences"}
    FORCONF -- "not in DB" --> WALK["walk_conference(conf)"]
    WALK --> GETSUB["get_submissions(conf)"]
    GETSUB --> FORSUB{"for submission in submissions"}
    FORSUB --> GETAUTH["get_authorids(submission)"]
    GETAUTH --> FORAUTH{"for author_id in authorids"}
    FORAUTH -- "not in DB" --> GETPROF["get_profile(author_id)"]
    GETPROF -- "found" --> ADDPROF["DB.add_profile(profile)"]
    GETPROF -- "not found" --> ADDEMPTY["DB.add_empty_profile(author_id)"]
    FORSUB --> ADDPAPER["DB.add_paper(submission, conf)"]
    WALK --> ADDCONF["DB.add_conference(conf, len(submissions))"]
    ADDPROF --> AUTHORS[("authors table")]
    ADDEMPTY --> AUTHORS
    ADDPAPER --> PAPERS[("papers table")]
    ADDPAPER --> EDGES[("author_paper_edges table")]
    ADDPAPER --> REVIEWS[("reviews table")]
    ADDCONF --> CONFS[("conferences table")]
    style Spider fill:#e0f7fa,stroke:#26c6da,stroke-width:2px
    style DB fill:#f1f8e9,stroke:#8bc34a,stroke-width:2px
```

## How to Run

### Requirements

```shell
python==3.12
openreview-py>=1.50
requests>=2.22.0
tqdm
```

### Set Account

Edit [src/main.py](src/main.py), set OpenReview Account and Password

```python
import openreview
from openReviewSpider import OpenReviewSpider
    
if __name__ == '__main__':
    client = openreview.api.OpenReviewClient(
    baseurl='https://api2.openreview.net',
    username='your_username',  # Replace with your OpenReview username
    password='your_password'  # Replace with your OpenReview password
    )
    
    spider = OpenReviewSpider(client, '~/data/')
    spider.run()
    print("OpenReview Spider finished running.")

```

### Run

```shell
cd src
python main.py
```

## Data Analysis

see [DataVisualization](./DataVisualization.md)



