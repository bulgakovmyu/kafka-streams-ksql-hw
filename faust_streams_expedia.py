import faust
from datetime import datetime

app = faust.App("ExpediaTest", broker="kafka://localhost:9092", value_serializer="json")


class Expedia(faust.Record, validation=True):
    id: float
    date_time: str
    site_name: int
    user_location_country: int
    user_location_region: int
    user_location_city: int
    user_id: int
    is_mobile: int
    is_package: int
    channel: int
    srch_ci: str
    srch_co: str
    srch_adults_cnt: int
    srch_children_cnt: int
    srch_rm_cnt: int
    srch_destination_id: int
    srch_destination_type_id: int
    hotel_id: float


class ExpediaExt(Expedia):
    stay_category: str


exp_topic = app.topic("copy_of_expedia_json", value_type=Expedia)

extended_exp_topic = app.topic(
    "extended_copy_of_expedia_json", internal=False, partitions=3, value_type=ExpediaExt
)


def is_appropriate_date_format(date_string):
    try:
        date_from_string = datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except Exception:
        return False


def get_stay_category(checkin: str, checkout: str) -> str:
    is_date_none = checkin is None or checkout is None
    if not is_date_none:
        is_date_valid = is_appropriate_date_format(
            checkin
        ) and is_appropriate_date_format(checkout)
        if is_date_valid:
            checkin_date = datetime.strptime(checkin, "%Y-%m-%d")
            checkout_date = datetime.strptime(checkout, "%Y-%m-%d")
            days_of_stay = (checkout_date - checkin_date).days
            if (days_of_stay >= 1) and (days_of_stay <= 4):
                return "Short stay"
            elif (days_of_stay >= 5) and (days_of_stay <= 10):
                return "Standard stay"
            elif (days_of_stay >= 11) and (days_of_stay <= 14):
                return "Standard extended stay"
            elif days_of_stay > 14:
                return "Long stay"
        else:
            return "Invalid datestring"
    else:
        return "Empty datestring"


@app.agent(exp_topic)
async def print_expedia(messages):
    async for message in messages:
        print(f"Data recieved is {message.asdict()}")

        new_message = ExpediaExt(
            **message.asdict(),
            stay_category=get_stay_category(message.srch_ci, message.srch_co),
        )
        await extended_exp_topic.send(value=new_message)
        print(f"{new_message} --- sent to filtered_copy_of_expedia_json topic")


app.main()
