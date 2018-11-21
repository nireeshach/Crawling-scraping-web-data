import argparse
import os
import os.path
import lxml.html
from urllib.parse import urlparse, urljoin

import requests
import pandas as pd
import re
import numpy as np

newdf = pd.DataFrame()


class InvalidResponse(Exception):
    def __init__(self, message):

        # Call the base class constructor with the parameters it needs
        super().__init__(message)


def get_domain(url):
    parsed_uri = urlparse(url)
    uri_path = parsed_uri.path
    uri_req = uri_path.rpartition("/")[0].split("your")
    domain = "{uri.scheme}://{uri.netloc}".format(uri=parsed_uri)
    domain1 = domain + uri_req[0]
    return domain1


def construct_url(parent_url, url):
    if not url.startswith("http"):
        domain = get_domain(parent_url)
        split_url = url.split("../")

        if len(split_url) > 2:
            join_url = urljoin(domain, url.split("../")[2])
        else:
            join_url = urljoin(domain, split_url[1])
        return join_url
    else:
        return url


def domain_fun(products_details, heading):

    # fun for table_name = domintable

    p1 = products_details[0]

    df = pd.read_html(lxml.html.tostring(p1).strip(), header=0, index_col=0)[0]

    df_transposed = df.transpose()
    df_transposed = df_transposed.groupby(level=0, axis=1).first()
    df_transposed.insert(loc=0, column="printer_name", value=heading[0])

    return df_transposed


def data_details1(data_details, data_heading):

    # fun for tablename ghoti for two tables

    df_data = pd.read_html(
        lxml.html.tostring(data_details[0]).strip(), header=0, index_col=0, na_values=0
    )[0]

    df_data1 = pd.read_html(
        lxml.html.tostring(data_details[1]).strip(), header=0, index_col=0
    )[0]

    df_data.reset_index(drop=True, inplace=True)
    df_data.set_index(["Printing stock"], inplace=True)

    df_data1.reset_index(drop=True, inplace=True)
    df_data1.set_index(["Printing stock"], inplace=True)

    df_data_transposed = df_data.transpose()
    df_data_transposed = df_data_transposed.groupby(level=0, axis=1).first()

    # df_data_transposed.drop_duplicates(inplace=True)
    df_data_transposed.insert(loc=0, column="printer_name", value=data_heading[0])

    df_data1_transposed = df_data1.T
    df_data1_transposed = df_data1_transposed.groupby(level=0, axis=1).first()
    df_data1_transposed.insert(loc=0, column="printer_name", value=data_heading[1])

    df_merged = pd.concat([df_data_transposed, df_data1_transposed])

    return df_merged


def data_details2(data_details, heading):

    # fun for tablename ghoti for one tables

    df_data = pd.read_html(
        lxml.html.tostring(data_details[0]).strip(), header=0, index_col=0, na_values=0
    )[0]
    df_data.reset_index(drop=True, inplace=True)
    df_data.set_index(["Printing stock"], inplace=True)
    df_data_transposed = df_data.transpose()
    df_data_transposed = df_data_transposed.groupby(level=0, axis=1).first()
    df_data_transposed.insert(loc=0, column="printer_name", value=heading[0])

    return df_data_transposed


def crawl_capabilities(url, html=None):

    global newdf

    try:
        if not html:
            resp = requests.get(url)
            if not resp.status_code == 200:
                raise InvalidResponse(
                    "Request to the given url, failed with InvalidResponse"
                )
            html = lxml.html.fromstring(resp.text.strip())

        domain_xpath = '//table[@class="domtable"]'
        domain_xpath_heading = '//div[contains(@class,"titleunderline")]//h2/text()'
        domain_xpath1 = '//table[@class = "gothic"]'
        domain_xpath1_heading = (
            '//div[contains(@class,"headlines underline")]//h4/text()'
        )

        products_details = html.xpath(domain_xpath)
        heading = html.xpath(domain_xpath_heading)
        data_details = html.xpath(domain_xpath1)
        data_heading = html.xpath(domain_xpath1_heading)

        if products_details:

            df = domain_fun(products_details, heading)
            newdf = pd.concat([newdf, df], sort=False)

            # newdf.merge(df, how="outer", indicator=True)

        else:

            if len(data_details) > 1:

                df = data_details1(data_details, data_heading)
                newdf = pd.concat([newdf, df], sort=False)

                # newdf.merge(df, how="outer", indicator=True, left_index=False)

            elif len(data_details) > 0 and len(data_details) <= 1:

                df = data_details2(data_details, heading)
                newdf = pd.concat([newdf, df], sort=False)

                # newdf.merge(df, how="outer", indicator=True)
            else:
                pass

        print(url, html)

    except requests.exceptions.ConnectionError as e:
        print("Failed to get connection to given URL: {}".format(url))
    except requests.exceptions.RequestException as e:
        raise e


def crawl_techdata(urls):

    resp = requests.get(urls)

    html = lxml.html.fromstring(resp.text.strip())

    techdata_xpath = '//div[@class="tabs haspadding"]//li[contains(@class,"col-sm-6")]/a[contains(@href,"techn")]/@href'

    techdata_urls = html.xpath(techdata_xpath)

    techdata1_xpath = (
        '//div[@class="headlines underline"]//h4[contains(text(),"Technical")]/text()'
    )
    techdata1_urls = html.xpath(techdata1_xpath)

    if techdata_urls:

        capabilities_url = construct_url(urls, techdata_urls[0])

        crawl_capabilities(capabilities_url)

    elif techdata1_urls:

        crawl_capabilities(urls, html)

    else:
        pass


def crawl_url(parent_url):

    try:
        resp = requests.get(parent_url)
        if not resp.status_code == 200:
            raise InvalidResponse(
                "Request to the given url, failed with InvalidResponse"
            )

        html = lxml.html.fromstring(resp.text.strip())

        url_xpath = '//div[@class="row link-list"]//div[contains(@class,"col-lg-4")]//a[@class="link-to"]/@href'

        url1 = html.xpath(url_xpath)

        for url in url1:

            urls = construct_url(parent_url, url)

            crawl_techdata(urls)

    except requests.exceptions.ConnectionError as e:
        print("Failed to get connection to given URL: {}".format(url))
    except requests.exceptions.RequestException as e:
        raise e

    new_df = newdf.dropna(axis=1, how="all")
    # new_df.insert(0, "value", 1)
    new_df.reset_index(level=0, inplace=True)
    # lst = new_df["index"].tolist()
    # lst1 = [
    #     x.repalce(x, "") for x in new_df["index"].tolist() if x.startswith("Unnamed")
    # ]
    new_df.rename(columns={"index": "sub_category"}, inplace=True)
    fnl_df = new_df.set_index(["printer_name"], drop=True)
    fnl_df = fnl_df.replace(["-"], np.nan)
    fnl_df = fnl_df.replace(["Unnamed: 2", "Unnamed: 1"], np.nan)
    fnl_df.to_csv("heidelberg_capabilities.csv")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-u", "--url", dest="url", help="Recipe url to get Ingredients data"
    )
    args = parser.parse_args()
    crawl_url(args.url)
