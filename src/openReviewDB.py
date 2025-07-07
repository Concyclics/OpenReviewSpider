import sqlite3
import os
import json
import pandas as pd
import time

class OpenReviewDB:
    def __init__(self, path):
        # Set database path
        self.db_path = path + '/openreview.db'
        # Ensure metadata folders exist
        self.initialize_metadata_folder(path + '/profiles')
        self.initialize_metadata_folder(path + '/papers')
        self.initialize_metadata_folder(path + '/reviews')
        # Connect to SQLite database
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        # Sets to keep track of existing entries
        self.existed_authors = set()
        self.existed_papers = set()
        self.existed_conferences = set()
        # Initialize tables and load existing data
        self.initialize()

    def __del__(self):
        # Commit and close connection on deletion
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def load_existed_authors(self):
        # Load existing author IDs from database
        self.cursor.execute("SELECT id FROM authors")
        rows = self.cursor.fetchall()
        for row in rows:
            self.existed_authors.add(row[0])

    def load_existed_papers(self):
        # Load existing paper IDs from database
        self.cursor.execute("SELECT id FROM papers")
        rows = self.cursor.fetchall()
        for row in rows:
            self.existed_papers.add(row[0])
            
    def load_existed_conferences(self):
        # Load existing conference IDs from database
        self.cursor.execute("SELECT id FROM conferences")
        rows = self.cursor.fetchall()
        for row in rows:
            self.existed_conferences.add(row[0])

    def initialize(self):
        # Initialize tables and load existing data
        self.initialize_table()
        self.load_existed_authors()
        self.load_existed_papers()
        self.load_existed_conferences()

    def initialize_table(self):
        # Create tables if they do not exist
        if not self.table_exists('authors'):
            self.create_authors_table()
        if not self.table_exists('papers'):
            self.create_papers_table()
        if not self.table_exists('reviews'):
            self.create_reviews_table()
        if not self.table_exists('author_paper_edges'):
            self.create_author_paper_edges_table()
        if not self.table_exists('conferences'):
            self.create_conferences_table()
        self.conn.commit()

    def table_exists(self, table_name):
        # Check if a table exists in the database
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return self.cursor.fetchone() is not None

    def hash_metadata(self, metadata):
        # Generate SHA256 hash for metadata
        import hashlib

        metadata_str = str(metadata).encode('utf-8')
        return hashlib.sha256(metadata_str).hexdigest()

    def initialize_metadata_folder(self, metadata_folder):
        # Create metadata folder if it does not exist
        if not os.path.exists(metadata_folder):
            os.makedirs(metadata_folder)
            
    def create_conferences_table(self):
        # Create conferences table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS conferences (
                id TEXT PRIMARY KEY,
                submissions INTEGER
            )
        ''')

    def create_authors_table(self):
        # Create authors table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS authors (
                id TEXT PRIMARY KEY,
                name TEXT,
                position TEXT,
                affiliation TEXT,
                metadata_path TEXT,
                metadata_hash TEXT
            )
        ''')

    def create_papers_table(self):
        # Create papers table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS papers (
                id TEXT PRIMARY KEY,
                title TEXT,
                conference TEXT,
                decision TEXT,
                avg_rating REAL,
                num_reviews INTEGER,
                min_rating INTEGER,
                max_rating INTEGER,
                cdate INTEGER,
                number INTEGER,
                paperhash TEXT,
                abstract TEXT,
                metadata_path TEXT,
                metadata_hash TEXT
            )
        ''')

    def create_reviews_table(self):
        # Create reviews table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS reviews (
                paper_id TEXT PRIMARY KEY,
                rating INTEGER,
                comment TEXT,
                metadata_path TEXT,
                metadata_hash TEXT,
                FOREIGN KEY (paper_id) REFERENCES papers (id)
            )
        ''')

    def create_author_paper_edges_table(self):
        # Create author-paper edges table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS author_paper_edges (
                author_id TEXT,
                paper_id TEXT,
                cdate INTEGER,
                FOREIGN KEY (author_id) REFERENCES authors (id),
                FOREIGN KEY (paper_id) REFERENCES papers (id)
            )
        ''')
        
    def query_co_authors_by_date(self, author_id, start_date=0, end_date=None):
        if end_date is None:
            #12位 timestamp for current time
            end_date = int(time.time() * 1000)
        # Step 1: Find all paper_ids for the given author_id in the date range
        self.cursor.execute('''
            SELECT paper_id FROM author_paper_edges
            WHERE author_id = ? AND cdate >= ? AND cdate <= ?
        ''', (author_id, start_date, end_date))
        paper_ids = [row[0] for row in self.cursor.fetchall()]
        if not paper_ids:
            return pd.DataFrame(columns=['id', 'name', 'position', 'affiliation', 'count', 'last_date'])
        # Step 2: Find all co-author_ids for these papers, excluding the input author_id
        placeholders = ','.join('?' for _ in paper_ids)
        query = f'''
            SELECT a.id, a.name, a.position, a.affiliation,
                   COUNT(*) as count, MAX(ape.cdate) as last_date
            FROM authors a
            JOIN author_paper_edges ape ON a.id = ape.author_id
            WHERE ape.paper_id IN ({placeholders})
              AND a.id != ?
            GROUP BY a.id, a.name, a.position, a.affiliation
        '''
        params = paper_ids + [author_id]
        self.cursor.execute(query, params)
        rows = self.cursor.fetchall()
        df = pd.DataFrame(rows, columns=['id', 'name', 'position', 'affiliation', 'count', 'last_date'])
        return df

    def add_conference(self, conference_id, submissions):
        # Add a conference if not already present
        if conference_id in self.existed_conferences:
            return

        self.existed_conferences.add(conference_id)
        self.cursor.execute('''
            INSERT OR REPLACE INTO conferences (id, submissions)
            VALUES (?, ?)
        ''', (conference_id, submissions))
        self.conn.commit()

    def add_profile(self, profile):
        # Add an author profile if not already present
        author_id = profile.id
        if author_id in self.existed_authors:
            return

        metadata_path = f'profiles/{author_id}.json'
        metadata_hash = self.hash_metadata(profile.to_json())
        # Check if metadata file exists and is unchanged
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                existing_metadata = json.load(f)
                if self.hash_metadata(existing_metadata) == metadata_hash:
                    return
        else:
            with open(metadata_path, 'w') as f:
                json.dump(profile.to_json(), f)

        # Extract author details
        name = profile.content['names'][0]['fullname']
        try:
            position = profile.content['history'][0]['position']
            affiliation = profile.content['history'][0]['institution']['name']
        except (KeyError, IndexError):
            position = 'N/A'
            affiliation = 'N/A'

        self.existed_authors.add(author_id)
        self.cursor.execute('''
            INSERT OR REPLACE INTO authors (id, name, position, affiliation, metadata_path, metadata_hash)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (author_id, name, position, affiliation, metadata_path, metadata_hash))
        self.conn.commit()
        
    def add_empty_profile(self, author_id):
        # Add an empty author profile if not already present
        if author_id in self.existed_authors:
            return
        
        metadata_path = f'profiles/{author_id}.json'
        if '@' not in author_id:
            metadata_hash = self.hash_metadata({'id': author_id})
        else:
            metadata_hash = self.hash_metadata({'id': author_id, 'content': {'emails': [author_id]}})
        # Check if metadata file exists and is unchanged
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                existing_metadata = json.load(f)
                if self.hash_metadata(existing_metadata) == metadata_hash:
                    return
        else:
            with open(metadata_path, 'w') as f:
                if '@' not in author_id:
                    json.dump({'id': author_id}, f)
                else:
                    json.dump({'id': author_id, 'content': {'emails': [author_id]}}, f)
                
        self.existed_authors.add(author_id)
        self.cursor.execute('''
            INSERT OR REPLACE INTO authors (id, name, position, affiliation, metadata_path, metadata_hash)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (author_id, author_id, 'N/A', 'N/A', metadata_path, metadata_hash))
        self.conn.commit()

    def add_paper(self, paper, conference):
        # Add a paper and its reviews if not already present
        paper_id = paper.id
        if paper_id in self.existed_papers:
            return

        # Generate paperhash for file naming
        paper_hash_tmp = paper.content.get('paperhash', {}).get('value', '')
        if '/' in paper_hash_tmp:
            paper_hash_tmp = paper_hash_tmp.replace('/', '_')
        if len(paper_hash_tmp) > 128:
            paper_hash_tmp = paper_hash_tmp[:128]
        paperhash = paper_hash_tmp + "|" + conference.replace('/', '_') + '|' + str(paper.number)
        metadata_path = f'papers/{paperhash}.json'
        metadata_hash = self.hash_metadata(paper.to_json())
        # Check if metadata file exists and is unchanged
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                existing_metadata = json.load(f)
                if self.hash_metadata(existing_metadata) == metadata_hash:
                    return
        else:
            with open(metadata_path, 'w') as f:
                json.dump(paper.to_json(), f)

        # Extract paper details
        title = paper.content.get('title', {}).get('value', '')
        abstract = paper.content.get('abstract', {}).get('value', '')
        decision = 'N/A'
        # Find decision in reviews
        for review in paper.details['directReplies']:
            str_review = str(review).lower()
            if 'decision' in str_review:
                decision = review['content'].get('decision', {}).get('value', '')
        if decision == '' or decision is None:
            decision = 'N/A'
        self.existed_papers.add(paper_id)
        timestamp = paper.cdate

        # Collect ratings from reviews
        ratings = []
        for review in paper.details['directReplies']:
            str_review = str(review).lower()
            if 'rating' in str_review:
                rating = review['content'].get('rating', {}).get('value', None)
                if rating is not None:
                    if isinstance(rating, str):
                        try:
                            rating = int(rating.strip())
                        except ValueError:
                            for rating_tmp in rating.strip().split():
                                try:
                                    rating = int(rating_tmp)
                                    break
                                except ValueError:
                                    continue
                    if isinstance(rating, int):
                        ratings.append(rating)

        avg_rating = sum(ratings) / len(ratings) if ratings else None
        num_reviews = len(ratings)
        min_rating = min(ratings) if ratings else None
        max_rating = max(ratings) if ratings else None

        # Insert paper into database
        self.cursor.execute('''
            INSERT OR REPLACE INTO papers (
                id, title, conference, decision, avg_rating, num_reviews,
                min_rating, max_rating, cdate, number, paperhash, abstract,
                metadata_path, metadata_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            paper_id, title, conference, decision, avg_rating, num_reviews,
            min_rating, max_rating, timestamp, paper.number, paperhash,
            abstract, metadata_path, metadata_hash
        ))

        # Insert author-paper edges
        authorids = paper.content.get('authorids', {}).get('value', [])
        for author_id in authorids:
            self.cursor.execute('''
                INSERT OR REPLACE INTO author_paper_edges (author_id, paper_id, cdate)
                VALUES (?, ?, ?)
            ''', (author_id, paper_id, timestamp))

        # Save reviews metadata
        reviews = paper.details['directReplies']
        reviews_metadata_path = f'reviews/{paperhash}.json'
        reviews_metadata_hash = self.hash_metadata(reviews)
        if os.path.exists(reviews_metadata_path):
            with open(reviews_metadata_path, 'r') as f:
                existing_reviews = json.load(f)
                if self.hash_metadata(existing_reviews) == reviews_metadata_hash:
                    return
        else:
            with open(reviews_metadata_path, 'w') as f:
                json.dump(reviews, f)

        # Insert reviews into database
        self.cursor.execute('''
            INSERT OR REPLACE INTO reviews (
                paper_id, rating, comment, metadata_path, metadata_hash
            )
            VALUES (?, ?, ?, ?, ?)
        ''', (
            paper_id, avg_rating, str(reviews),
            reviews_metadata_path, reviews_metadata_hash
        ))

        self.conn.commit()
