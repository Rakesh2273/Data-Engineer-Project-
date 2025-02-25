import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO
import logging

def album(data):
    return [
        {
            'album_id': row['track']['album']['id'],
            'name': row['track']['album']['name'],
            'release_date': row['track']['album']['release_date'],
            'total_tracks': row['track']['album']['total_tracks'],
            'url': row['track']['album']['external_urls']['spotify']
        }
        for row in data['items']
    ]

def artist(data):
    artist_list = []
    for row in data['items']:
        for artist in row['track']['artists']:
            artist_list.append({
                'artist_id': artist['id'],
                'artist_name': artist['name'],
                'external_url': artist['external_urls']['spotify']
            })
    return artist_list

def song(data):
    return [
        {
            'song_id': row['track']['id'],
            'song_name': row['track']['name'],
            'duration_ms': row['track']['duration_ms'],
            'url': row['track']['external_urls']['spotify'],
            'popularity': row['track']['popularity'],
            'song_added': row['added_at'],
            'album_id': row['track']['album']['id'],
            'artist_id': row['track']['album']['artists'][0]['id']
        }
        for row in data['items']
    ]

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    
    s3 = boto3.client('s3')
    bucket = 'spotify-etl-project-5may'
    prefix = 'raw_data/to_processed/'
    
    try:
        spotify_data = []
        spotify_keys = []
        
        for file in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', []):
            file_key = file['Key']
            if file_key.endswith('.json'):
                response = s3.get_object(Bucket=bucket, Key=file_key)
                jsonObject = json.loads(response['Body'].read())
                spotify_data.append(jsonObject)
                spotify_keys.append(file_key)
                
        for data in spotify_data:
            album_df = pd.DataFrame(album(data)).drop_duplicates(subset='album_id')
            artist_df = pd.DataFrame(artist(data)).drop_duplicates(subset='artist_id')
            song_df = pd.DataFrame(song(data)).drop_duplicates(subset='song_id')
            
            song_df['song_added'] = pd.to_datetime(song_df['song_added'])
            album_df['release_date'] = pd.to_datetime(album_df['release_date'])
            
            for df, key_prefix in zip([song_df, album_df, artist_df],
                                      ['song_data/song_transformed_', 'album_data/album_transformed_', 'artist_data/artist_transformed_']):
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                file_key = f'transformed_data/{key_prefix}{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'
                s3.put_object(Bucket=bucket, Key=file_key, Body=csv_buffer.getvalue())
                logger.info(f"Uploaded {file_key}")
        
        s3_resource = boto3.resource('s3')
        for key in spotify_keys:
            copy_source = {'Bucket': bucket, 'Key': key}
            s3_resource.meta.client.copy(copy_source, bucket, f'raw_data/processed/{key.split('/')[-1]}')
            s3_resource.Object(bucket, key).delete()
            logger.info(f"Processed and moved {key}")
        
        return {'statusCode': 200, 'body': json.dumps('ETL job completed successfully.')}
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f"Error: {str(e)}")}
