import requests
import sys, os
import json

from src.utils.basic_authentication import base64_authentication

class CvrRepository:

    def __init__(self, username: str, password: str, session:requests.Session=None):
        self._base_url = "http://distribution.virk.dk"
        self.username = username
        self.password = password
        self.session = session or requests.Session()

    @property
    def base_url(self):
        return self._base_url # Expose the base_url as a read-only property

    def _set_header(self):
        return {'Content-Type': 'application/json',
                'Authorization': base64_authentication(username=self.username, password=self.password)
            }
    

    def get_scroll_id(self, cvr_data: requests.Response) -> str:

        try:
            scroll_id = cvr_data.json().get('_scroll_id')
            if not scroll_id:
                raise KeyError('_scroll_id not found in response.')
            return scroll_id

        except AttributeError as e:
            raise AttributeError(f"Invalid response from cvr virk endpoint!: {e}")


    def extract_cvr_virksomhed_data(self, query: dict, scroll: str='1m') -> dict:

        # Virksomheds endpoint
        virksomheds_endpoint = f"{self.base_url}/cvr-permanent/virksomhed/_search?scroll={scroll}"

        response = self.session.get(
            url=virksomheds_endpoint,
            headers=self._set_header(),
            data=query
            )
        
        response.raise_for_status() # Raise exception if status code is not 200
        return response

    
    def scroll(self, scroll_id: str, scroll_minutes: str='2m') -> dict:

        # Scroll endpoint
        scroll_endpoint = f"{self.base_url}/_search/scroll"

        # Payload for scroll request
        payload = json.dumps({
            "scroll": scroll_minutes,
            "scroll_id": scroll_id
        })

        # response from scoll
        response = requests.get(
            url=scroll_endpoint,
            headers=self._set_header(),
            data=payload
            )
        
        response.raise_for_status() # Raise exception if status code is not 200
        # Use for requesting the scroll call
        return response