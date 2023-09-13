from datetime import datetime, timedelta
from json.decoder import JSONDecodeError
from os.path import expanduser
from time import sleep
from typing import Any, Optional
from urllib.parse import unquote

import mariadb
import pandas as pd
import requests

import pywikibot as pwb


WDQS_ENDPOINT = 'https://query.wikidata.org/sparql'
WDQS_HEADERS = {
    'User-Agent': f'{requests.utils.default_headers()["User-Agent"]} (Wikidata bot' \
                   ' by User:MisterSynergy; mailto:mister.synergy@yahoo.com)'
}
REPLICA_PARAMS = {
    'host' : 'wikidatawiki.analytics.db.svc.wikimedia.cloud',
    'database' : 'wikidatawiki_p',
    'default_file' : f'{expanduser("~")}/replica.my.cnf'
}

BLACKLIST_SANDBOX = [  # Sandbox and Tour items
    'Q4115189',
    'Q13406268',
    'Q15397819',
    'Q16943273',
    'Q17339402',
    'Q85409596',
    'Q85409446',
    'Q85409310',
    'Q85409163',
    'Q85408938',
    'Q85408509'
]
BLACKLIST_TECHNICAL = [
    'wdt:P31/wdt:P279* wd:Q4167410',  # Wikimedia disambiguation page
    'wdt:P31/wdt:P279* wd:Q11266439',  # Wikimedia template
    'wdt:P31/wdt:P279* wd:Q4167836'  # Wikimedia category
]

PAGE_TITLE = 'Wikidata:Main Page/Popular'
PAGE_TITLE_WITHOUT_NS = 'Main_Page/Popular'
PAGE_NAMESPACE = 4

DAYS = 3  # number of days to consider
MIN_ACTORS = 3  # min number of actors per item page
LIMIT = 7  # max number of items listed on popular page


class Replica:
    def __init__(self):
        self.replica = mariadb.connect(**REPLICA_PARAMS)
        self.cursor = self.replica.cursor(dictionary=True)


    def __enter__(self):
        return self.cursor


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.replica.close()


    @staticmethod
    def query_mediawiki(query:str) -> list[dict[str, Any]]:
        with Replica() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

        return result


    @staticmethod
    def query_mediawiki_to_dataframe(query:str) -> pd.DataFrame:
        result = Replica.query_mediawiki(query)

        df = pd.DataFrame(
            data=result
        )

        return df


def query_wdqs(query:str) -> dict[str, Any]:
    response = requests.post(
        url=WDQS_ENDPOINT,
        data={
            'query' : query,
            'format' : 'json'
        },
        headers=WDQS_HEADERS
    )
    sleep(1)
    try:
        payload = response.json()
    except JSONDecodeError as exception:
        raise RuntimeWarning('Cannot decode payload') from exception

    return payload


def ask_wdqs(query:str) -> bool:
    try:
        payload = query_wdqs(query)
    except RuntimeWarning:
        return False

    bool_response = payload.get('boolean', False)

    return bool_response


def query_image_from_wdqs(qids:list[str]) -> Optional[tuple[str, str]]:
    query = f"""SELECT ?item (SAMPLE(?image) AS ?img) WHERE {{
  VALUES ?item {{ wd:{' wd:'.join(qids)} }}
  OPTIONAL {{ ?item wdt:P18 ?image }}
}} GROUP BY ?item"""

    payload = query_wdqs(query)
    for row in payload.get('results', {}).get('bindings', []):
        qid = row.get('item', {}).get('value', '')[len('http://www.wikidata.org/entity/'):]
        image = row.get('img', {}).get('value')
        if image:
            image = unquote(image[len('http://commons.wikimedia.org/wiki/Special:FilePath/'):])
            return (qid, image)

    return None


def query_technical_item(qid:str) -> bool:
    is_technical_item = False

    for fragment in BLACKLIST_TECHNICAL:
        query = f"""ASK {{ wd:{qid} {fragment} }}"""
        is_technical_item = ask_wdqs(query)
        if is_technical_item:
            break

    return is_technical_item


def query_revisions() -> pd.DataFrame:
    start_timestamp = int((datetime.now()-timedelta(days=DAYS)).strftime('%Y%m%d%H%M%S'))

    query = f"""SELECT
  rc_id,
  CONVERT(rc_title USING utf8) AS qid,
  CONVERT(comment_text USING utf8) AS edit_summary,
  actor_id
FROM
  recentchanges
    JOIN actor_recentchanges ON rc_actor=actor_id
    JOIN comment_recentchanges ON rc_comment_id=comment_id
WHERE
  rc_namespace=0
  AND rc_new_len>rc_old_len
  AND rc_bot=0
  AND actor_user IS NOT NULL
  AND rc_deleted=0
  AND rc_source='mw.edit'
  AND rc_timestamp>{start_timestamp}"""

    df = Replica.query_mediawiki_to_dataframe(query)
    return df


def query_change_tags(min_rc_id:int) -> pd.DataFrame:
    query = f"""SELECT
  rc_id,
  CONVERT(ctd_name USING utf8) AS tag_name
FROM
  recentchanges
    JOIN change_tag ON rc_id=ct_rc_id
    JOIN change_tag_def ON ct_tag_id=ctd_id
WHERE
  rc_id>={min_rc_id:d}
"""

    df = Replica.query_mediawiki_to_dataframe(query)

    return df


def query_currently_listed_items() -> list[str]:
    query = f"""SELECT
  CONVERT(pl_title USING utf8) AS pl_title
FROM
  pagelinks
    JOIN page ON pl_from=page_id
WHERE
  page_namespace={PAGE_NAMESPACE:d}
  AND page_title='{PAGE_TITLE_WITHOUT_NS}'
  AND pl_namespace=0"""

    result = Replica.query_mediawiki(query)
    currently_listed_items = [ dct.get('pl_title', '') for dct in result ]

    return currently_listed_items


def get_displayable_items() -> list[str]:
    revisions = query_revisions()
    change_tags = query_change_tags(revisions['rc_id'].min())

    # remove revisions with a change tag that contains "OAuth" (automated editing)
    rc_ids_to_ignore = change_tags.loc[change_tags['tag_name'].str.contains('OAuth'), 'rc_id']
    revisions = revisions.loc[~revisions['rc_id'].isin(rc_ids_to_ignore)]

    # aggregate items by number of editors
    actor_cnt = revisions[['qid', 'actor_id']].groupby(
        by='qid'
    ).aggregate(
        {'actor_id' : pd.Series.nunique}
    ).reset_index()
    actor_cnt.rename(columns={'actor_id' : 'actor_cnt'}, inplace=True)
    top_author_items = actor_cnt.loc[actor_cnt['actor_cnt'] >= MIN_ACTORS].reset_index()

    # list of revisions in items with MIN_ACTORS or more actors
    top_author_item_revisions = revisions.loc[revisions['qid'].isin(top_author_items['qid'])]

    # break up raw edit summaries in order to extract autogenerated "magic edit summaries"
    top_author_item_revisions = top_author_item_revisions.merge(
        right=top_author_item_revisions['edit_summary'].str.extract(
            pat=r'^\/\* ((?<!\*\/).+) \*\/ ?(.*)?',
            expand=True
        ),
        how='left',
        left_index=True,
        right_index=True
    )
    top_author_item_revisions.rename(
        columns={
            0 : 'edit_summary_magic',
            1 : 'edit_summary_free'
        },
        inplace=True
    )

    # agggregate items by number of different actions
    action_cnt = top_author_item_revisions[['qid', 'edit_summary_magic']].groupby(
        by='qid'
    ).aggregate(
        {'edit_summary_magic' : pd.Series.nunique}
    ).reset_index()
    action_cnt.rename(columns={'edit_summary_magic' : 'action_cnt'}, inplace=True)

    # compile list of items, ordered by action_cnt DESC, actor_cnt DESC
    df = action_cnt.merge(
        right=top_author_items,
        on='qid'
    ).sort_values(
        by=['action_cnt', 'actor_cnt'],
        ascending=False
    )
    df.drop(columns=['index'], inplace=True)

    # check whether item is on a blacklist
    df['blacklist_sandbox'] = df['qid'].isin(BLACKLIST_SANDBOX)
    df['blacklist_previous'] = df['qid'].isin(query_currently_listed_items())
    df['blacklist_technical'] = df['qid'].apply(func=query_technical_item)

    blacklist_filter = (df['blacklist_sandbox'] == False) \
                     & (df['blacklist_previous'] == False) \
                     & (df['blacklist_technical'] == False)

    # remove blacklisted items and limit to LIMIT members (number of displayed items)
    df = df.loc[blacklist_filter].head(LIMIT)

    return df['qid'].tolist()


def make_wikitext(qid_list:list[str], image:Optional[tuple[str,str]]) -> str:
    img_qid_text:dict[str, str] = {}

    if not image:
        img_header = '<nowiki />\n'
    else:
        img_qid, img_source = image
        img_qid_text[img_qid] = ' ({{I18n|pictured}})'
        img_header = f'<span style="float: {{{{dir|{{{{{{lang|{{{{int:lang}}}}}}}}}}|left|right}}}}; padding-top: 0.5em; padding-{{{{dir|{{{{{{lang|{{{{int:lang}}}}}}}}}}|right|left}}}}: 0.5em;">[[File:{img_source}|100px]]</span>\n'

    body = '\n'.join([ f'* {{{{Q|{qid}}}}}{img_qid_text.get(qid, "")}' for qid in qid_list])
    footer = '<span style="clear: {{dir|{{{lang|{{int:lang}}}}}|left|right}};"></span><noinclude>[[Category:Wikidata:Main Page]]</noinclude>'

    wikitext = f'{img_header}{body}{footer}'

    return wikitext


def write_to_wiki(wikitext:str) -> None:
    #print(wikitext)
    site = pwb.Site('wikidata', 'wikidata')
    page = pwb.Page(site, PAGE_TITLE)
    page.text = wikitext
    page.save(summary='upd', minor=False)


def main() -> None:
    qid_list = get_displayable_items()
    image = query_image_from_wdqs(qid_list)

    write_to_wiki(make_wikitext(qid_list, image))


if __name__=='__main__':
    main()
