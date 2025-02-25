import json
import os
import logging
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    
    # Get environment variables
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    bucket_name = os.environ.get('S3_BUCKET_NAME', 'spotify-etl-project-5may')
    
    if not client_id or not client_secret:
        logger.error("Spotify client credentials are missing.")
        return {
            'statusCode': 500,
            'body': json.dumps("Missing Spotify client credentials.")
        }
    
    try:
        client_credentials_mgr = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_mgr)
        
        playlist_link = 'https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF'
        playlist_URI = playlist_link.split('/')[-1]
        
        spotify_data = sp.playlist_tracks(playlist_URI)
        
        # Validate Spotify data
        if not spotify_data:
            logger.error("No data retrieved from Spotify API.")
            return {
                'statusCode': 500,
                'body': json.dumps("No data retrieved from Spotify API.")
            }
        
        client = boto3.client('s3')
        filename = f'spotify_raw_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.json'
        file_path = f'raw_data/to_processed/{filename}'
        
        client.put_object(
            Bucket=bucket_name,
            Key=file_path,
            Body=json.dumps(spotify_data),
            ContentType='application/json'
        )
        
        logger.info(f"File successfully uploaded: {file_path}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"File uploaded successfully: {file_path}")
        }
    
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
