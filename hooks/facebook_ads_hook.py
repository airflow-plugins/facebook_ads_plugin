from airflow.hooks.base_hook import BaseHook

from urllib.parse import urlencode
import requests
import time


class FacebookAdsHook(BaseHook):
    def __init__(self, facebook_ads_conn_id='facebook_ads_default'):
        self.facebook_ads_conn_id = facebook_ads_conn_id
        self.connection = self.get_connection(facebook_ads_conn_id)

        self.base_uri = 'https://graph.facebook.com'
        self.api_version = self.connection.extra_dejson['apiVersion'] or '2.10'
        self.access_token = self.connection.extra_dejson['accessToken'] or self.connection.password

    def get_insights_for_account_id(self, account_id, insight_fields, breakdowns, time_range, time_increment='all_days', level='ad', limit=100):
        payload = urlencode({
            'access_token': self.access_token,
            'breakdowns': ','.join(breakdowns),
            'fields': ','.join(insight_fields),
            'time_range': time_range,
            'time_increment': time_increment,
            'level': level,
            'limit': limit
        })

        response = requests.get('{base_uri}/v{api_version}/act_{account_id}/insights?{payload}'.format(
            base_uri=self.base_uri,
            api_version=self.api_version,
            account_id=account_id,
            payload=payload
        ))

        response.raise_for_status()
        response_body = response.json()
        insights = []

        while 'next' in response_body.get('paging', {}):
            time.sleep(1)
            insights.extend(response_body['data'])
            response = requests.get(response_body['paging']['next'])
            response.raise_for_status()
            response_body = response.json()

        insights.extend(response_body['data'])

        return insights
