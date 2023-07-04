import spotipy
import os 
from dotenv import load_dotenv
import pandas as pd 
from datetime import datetime 

load_dotenv()
spotify = spotipy.Spotify(client_credentials_manager=spotipy.oauth2.SpotifyClientCredentials(
            client_id = os.getenv('CLIENT_ID'),
            client_secret = os.getenv('CLIENT_SECRET')
                    ))

def get_artists_from_playlist(playlist_URI):
    ''' 
    :param playlist_URI: playlist URI
    :return: A dictionary of artists and their URIs for the playlist
    '''
    
    playlist_tracks = spotify.playlist_tracks(playlist_id = playlist_URI, market = 'GB')
    artists = {}

    for song in playlist_tracks['items']:
        if song['track']:
            name = song['track']['artists'][0]['name']
            uri = song['track']['artists'][0]['uri']
            artists[uri] = name
    return(artists)

def get_data(playlist):
    '''
    :param playlist: playlist URI passed onto get_artists_from_playlist() function
    :return A parquet file with songs, albums, an artists in playlist specified
    '''

    final_data_dictionary = {
    'Artist': [],
    'Album Name': [],
    'Year Released': [],
    'Album Length': []  }

    artists = get_artists_from_playlist(playlist)
    for artist_key in artists:
        artist_name = artists[artist_key]
    
        # pull out all albums by artist
        albums_by_artist = spotify.artist_albums(artist_id = artist_key, country = 'GB',album_type='album', limit=50)

        for album in albums_by_artist['items']:
            # all album data 
            release_date = album['release_date']
            album_name = album['name']
            artist = album['artists'][0]['name']
            album_uri = album['uri']

            album_duration = 0
            for song in spotify.album(album_uri, market = 'GB')['tracks']['items']:
                album_duration += song['duration_ms']

             # put final dictionary together
            final_data_dictionary['Artist'].append(artist_name)
            final_data_dictionary['Album Name'].append(album_name)
            final_data_dictionary['Year Released'].append(release_date)
            final_data_dictionary['Album Length'].append(album_duration)    
     
    output_file = pd.DataFrame.from_dict(final_data_dictionary)
  
    output_file = output_file.to_parquet('tmp/rock_albums.parquet')
      
    date = datetime.today()
    
    filename = f'{date.year}/{date.month}/{date.day}/rock_albums.parquet'
    