import spotipy
import os
import pandas as pd
from datetime import datetime
import boto3


def authenticate_spotify():
    try:
        clientID = os.environ["SPOTIFY_CLIENT_ID"]
        clientSecret = os.environ["SPOTIFY_CLIENT_SECRET"]

        # authenticate Spotipy
        spotify = spotipy.Spotify(
            client_credentials_manager=spotipy.oauth2.SpotifyClientCredentials(
                client_id=clientID, client_secret=clientSecret
            )
        )
        return spotify
    except:
        print("Cannot find credentials in environment")


def get_artists_from_playlist(playlist_URI):
    """
    From a spotify playlist URI returns a dictionary of all artists on that playlist.

    :param playlist_URI: playlist URI
    :return: A dictionary of artists and their URIs for the playlist
    """

    playlist_tracks = spotify.playlist_tracks(playlist_id=playlist_URI, market="GB")
    artists = {}

    for song in playlist_tracks["items"]:
        if song["track"]:
            name = song["track"]["artists"][0]["name"]
            uri = song["track"]["artists"][0]["uri"]
            artists[uri] = name
    return artists


def upload_to_s3(local_path, key, bucket):
    """
    Upload a file to S3
    Needs a boto3 session and resource to be set up and named s3
    """
    session = boto3.Session(profile_name="Admin_Profile", region_name="eu-west-2")

    s3 = session.resource("s3")

    try:
        date_today = datetime.today().strftime("%Y-%m-%d")

        s3_filename = f"{date_today}_{key}"

        s3.Bucket(bucket).upload_file(Filename=local_path, Key=s3_filename)

        print(f"Uploaded file {s3_filename} to {bucket}")

    except:
        print("Something went wrong - check inputs")


def get_data(playlist):
    """
    :param playlist: playlist URI passed onto get_artists_from_playlist() function
    :return A parquet file with songs, albums, an artists in playlist specified
    """
    # authenticate spotify first
    spotify = authenticate_spotify()

    final_data_dictionary = {
        "Artist": [],
        "Album Name": [],
        "Year Released": [],
        "Album Length": [],
    }

    artists = get_artists_from_playlist(playlist)
    for artist_key in artists:
        artist_name = artists[artist_key]

        # pull out all albums by artist
        albums_by_artist = spotify.artist_albums(
            artist_id=artist_key, country="GB", album_type="album", limit=50
        )

        for album in albums_by_artist["items"]:
            # all album data
            release_date = album["release_date"]
            album_name = album["name"]
            artist = album["artists"][0]["name"]
            album_uri = album["uri"]

            album_duration = 0
            for song in spotify.album(album_uri, market="GB")["tracks"]["items"]:
                album_duration += song["duration_ms"]

            # put final dictionary together
            final_data_dictionary["Artist"].append(artist_name)
            final_data_dictionary["Album Name"].append(album_name)
            final_data_dictionary["Year Released"].append(release_date)
            final_data_dictionary["Album Length"].append(album_duration)

    output_file = pd.DataFrame.from_dict(final_data_dictionary)

    output_file = output_file.to_parquet("tmp/rock_albums.parquet")

    # upload file to S3
    upload_to_s3(
        "tmp/rock_albums.parquet", key="rock_albums.parquet", bucket="spotify-etl-data"
    )
