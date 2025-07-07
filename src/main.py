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
