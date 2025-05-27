"""
Scraper de comentarios del canal de BM en YouTube.
Autor: Gabriela Polanco
Fecha: Mayo 2025
"""

import os
import csv
from dotenv import load_dotenv
from googleapiclient.discovery import build

# Load API key
load_dotenv()
key = os.getenv('YOUTUBE_API_KEY')

# Build YouTube client
youtube = build(
    serviceName='youtube',
    version='v3',
    developerKey=key
)

# Get United Nations Channel ID
request_id = youtube.search().list(
    part='snippet',
    q='United Nations',
    type='channel',
    maxResults=1
)
response_id = request_id.execute()
channel_id = response_id['items'][0]['id']['channelId']
print(f"Channel ID: {channel_id}")

# Fetch multiple videos and comments
all_comments = []
video_page_token = None
max_videos_to_try = 200
videos_tried = 0

while len(all_comments) < 500 and videos_tried < max_videos_to_try:
    request_uploads = youtube.search().list(
        part='snippet',
        channelId=channel_id,
        maxResults=50,
        order='date',
        pageToken=video_page_token,
        type='video'
    )
    response_uploads = request_uploads.execute()

    for video in response_uploads['items']:
        videos_tried += 1
        video_id = video['id']['videoId']
        title = video['snippet']['title']
        publishedAt = video['snippet']['publishedAt']
        print(
            f"\nFetching comments from: {title} (ID: {video_id})"
        )

        try:
            comment_page_token = None
            while len(all_comments) < 500:
                request_comments = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=100,
                    textFormat='plainText',
                    pageToken=comment_page_token
                )
                response_comments = request_comments.execute()

                if 'items' in response_comments:
                    for item in response_comments['items']:
                        comment_id = item['id']
                        text = item['snippet']['topLevelComment']['snippet'][
                            'textDisplay'
                        ]
                        text = text.replace('\n', ' ').replace('\r', ' ')
                        all_comments.append({
                            'comment_id': comment_id,
                            'text': text,
                            'video_id': video_id,
                            'video_title': title,
                            'video_publishedAt': publishedAt
                        })

                comment_page_token = response_comments.get('nextPageToken')
                if not comment_page_token:
                    break

        except Exception as e:
            print(
                f"Error fetching comments for {title}: {e}"
            )

    video_page_token = response_uploads.get('nextPageToken')
    if not video_page_token:
        break

print(
    f"\nTotal comments collected: {len(all_comments)} "
    f"from {videos_tried} videos"
)

# Save to CSV
os.makedirs('data', exist_ok=True)

with open('data/dataset.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            'comment_id',
            'text',
            'video_id',
            'video_title',
            'video_publishedAt'
        ]
    )
    writer.writeheader()
    writer.writerows(all_comments)

print("\nComments saved to data/dataset.csv")
