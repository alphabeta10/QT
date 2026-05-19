import asyncio
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from datetime import datetime
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data

BASE_URL = 'https://www.100ppi.com'
MONITOR_URL = 'https://www.100ppi.com/monitor/'


async def get_news_data_async(names=None):
    futures_news = get_mongo_table(database='futures', collection='futures_news')
    all_news = []

    crawler = PlaywrightCrawler(
      browser_type='chromium',
        headless=True,
    )

    @crawler.router.default_handler
    async def handle_page(context: PlaywrightCrawlingContext) -> None:
        page_type = context.request.user_data.get('page_type', 'monitor')
        product_name = context.request.user_data.get('product_name', 'unknown')
        product_url = context.request.user_data.get("url", "")
        
        await context.page.wait_for_load_state('networkidle')   
        
        if page_type == 'monitor':
            await handle_monitor_page(context)
        elif page_type == 'product':
            if product_url==context.request.url:
                print(context.request.user_data,context.request.url,'product')
                await handle_product_page(context, product_name)
            else:
                print(f"not eq {product_url} and r_url= {context.request.url}")
        # elif page_type == 'intelligence':
        #     await handle_intelligence_page(context, product_name, all_news)

    async def handle_monitor_page(context: PlaywrightCrawlingContext) -> None:
        await context.page.wait_for_selector('.list01', timeout=15000)
        await context.page.wait_for_selector('.list02', timeout=15000)
        
        list01_links = await context.page.query_selector_all('.list01 a')
        list02_links = await context.page.query_selector_all('.list02 a')
        
        all_links = list01_links + list02_links
        
        product_name_url_map = {}
        for link in all_links:
            text = await link.inner_text()
            href = await link.get_attribute('href')
            if text and href:
                product_name = text.strip()
                product_url = BASE_URL + href if not href.startswith('http') else href
                product_name_url_map[product_name] = product_url
        
        context.log.info(f'Found {len(product_name_url_map)} products')
        
        for product_name, product_url in product_name_url_map.items():
            if names and product_name not in names:
                continue
            print("enqueue product",product_name,product_url)
            await context.enqueue_links(urls=[product_url], user_data={'product_name': product_name, 'page_type': 'product','url':product_url})

    async def handle_product_page(context: PlaywrightCrawlingContext, product_name: str) -> None:
        context.log.info(f'Processing product: {product_name}')
        context.log.info(f'Processing {context.request.url} ...')

        
        try:
            await context.page.wait_for_selector('.price-new_more', timeout=15000)
            more_link = await context.page.query_selector('.price-new_more a')
            
            if more_link:
                href = await more_link.get_attribute('href')
                if href:
                    detail_url = BASE_URL + href if not href.startswith('http') else href
                    await context.enqueue_links(urls=[detail_url], user_data={'product_name': product_name, 'page_type': 'intelligence'})
        except Exception as e:
            context.log.warning(f'Could not find more link for {product_name}: {e}')

    async def handle_intelligence_page(context: PlaywrightCrawlingContext, product_name: str, all_news: list) -> None:
        context.log.info(f'Extracting intelligence for: {product_name}')
        
        try:
            await context.page.wait_for_selector('.dis-flex.w-qb', timeout=15000)
            news_items = await context.page.query_selector_all('.dis-flex.w-qb')
            
            cur_month = datetime.now().month
            cur_year = datetime.now().year
            cur_day = datetime.now().strftime('%Y-%m-%d')
            before_year = cur_year - 1
            
            for item in news_items:
                try:
                    p_tag = await item.query_selector('p')
                    span_tag = await item.query_selector('span')
                    
                    if p_tag and span_tag:
                        content = await p_tag.inner_text()
                        time_str = await span_tag.inner_text()
                        time_str = time_str.replace('\u2002', ' ')
                        
                        split_time = time_str.split(' ')
                        if len(split_time) == 2:
                            month_day = split_time[1]
                            data_month = month_day.split('-')[0]
                            if int(data_month) > cur_month:
                                year = before_year
                            else:
                                year = cur_year
                            dataday = month_day.split('-')[1]
                            date_str = f'{year}-{data_month}-{dataday} {split_time[0][:5]}:00'
                        else:
                            if ':' in time_str and '-' not in time_str:
                                date_str = f'{cur_day} {time_str[:5]}:00'
                            else:
                                context.log.warning(f'Time format error: {time_str}')
                                date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        dict_data = {
                            'content': content.strip(),
                            'time': date_str,
                            'data_type': 'goods_intelligence_news',
                            'metric_code': product_name
                        }
                        
                        all_news.append(UpdateOne(
                            {'content': dict_data['content'], 'metric_code': dict_data['metric_code'], 'data_type': dict_data['data_type']},
                            {'$set': dict_data},
                            upsert=True
                        ))
                        
                        context.log.info(f'Found news: {dict_data}')
                except Exception as e:
                    context.log.error(f'Error parsing news item: {e}')
                    continue
        except Exception as e:
            context.log.error(f'Could not extract intelligence for {product_name}: {e}')

    await crawler.run([MONITOR_URL])
    
    if len(all_news) > 0:
        mongo_bulk_write_data(futures_news, all_news)
        print(f'Successfully saved {len(all_news)} news items')


def get_news_data(names=None):
    asyncio.run(get_news_data_async(names))


if __name__ == '__main__':
    names = ['炼焦煤', '焦炭']
    get_news_data(names)
