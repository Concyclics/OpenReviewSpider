import openreview
import os
import json
import time
import tqdm
from openReviewDB import OpenReviewDB

class OpenReviewSpider:
    def __init__(self, client, DB_path):
        self.DB = OpenReviewDB(DB_path)
        self.DB_path = DB_path
        self.client = client
        self.delay = 0.1  # Delay to avoid hitting rate limits
            
    def __del__(self):
        if self.DB:
            del self.DB
            
    def get_profile(self, id):
        """
        Get a profile by ID.
        """
        time.sleep(self.delay)  # Avoid hitting rate limits
        try:
            profile = self.client.get_profile(id)
            if profile:
                return profile
        except:
            print(f'Failed to get profile for {id}')
            return None
    
    def get_submissions(self, conference_id, number=None):
        inv_bs = f'{conference_id}/-/Submission'
        
        time.sleep(self.delay)  # Avoid hitting rate limits
        submissions = self.client.get_all_notes(invitation=inv_bs, details='directReplies')
        return submissions

    def get_authorids(self, submission):
        """
        Get authors of a submission.
        """
        return submission.content.get('authorids', {}).get('value', [])
    
    def get_all_conferences(self):
        """
        Get all conferences.
        """
        venues = self.client.get_group(id='venues').members
        time.sleep(self.delay)  # Avoid hitting rate limits
        return venues
    
    def walk_conference(self, conf):
        submissions = self.get_submissions(conf)
        for submission in tqdm.tqdm(submissions, desc=f'Processing {conf} submissions'):
            authorids = self.get_authorids(submission)
            for author_id in authorids:
                if author_id not in self.DB.existed_authors:
                    if '@' not in author_id:
                        profile = self.get_profile(author_id)
                    else:
                        profile = None
                    if profile:
                        self.DB.add_profile(profile)
                    else:
                        self.DB.add_empty_profile(author_id)
            self.DB.add_paper(submission, conf)
        self.DB.add_conference(conf, len(submissions))
            
        print(f'Processed {conf}: {len(submissions)} submissions')
    
    def run(self):
        """
        Run the spider to collect data.
        """
        conferences = self.get_all_conferences()
        for conf in conferences:
            if conf in self.DB.existed_conferences:
                print(f'Skipping {conf}, already processed.')
                continue
            self.walk_conference(conf)

