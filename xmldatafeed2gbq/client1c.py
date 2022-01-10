import datetime
from datetime import timedelta

import bq_method
import win32com.client
from loguru import logger
from common_type import Struct

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)




def upload_from_1c(
    config, bqjsonservicefile, bqdataset, bqtable, datestock_start, datestock_end
):
    global query, choose
    struct_config = Struct(**config)
    CONSTR = f'Srvr="{struct_config.server}";Ref="{struct_config.infobase}";Usr="{struct_config.user}";Pwd="{struct_config.password}"'

    v83 = win32com.client.Dispatch("V83.COMConnector").Connect(CONSTR)
    q = get_query_fullstock()
    query = v83.NewObject("Query", q)
    filterList = []
    filterList.append(
        {
            "fieldname": "datestock",
            "operator": ">=",
            "value": datestock_start.strftime("%Y-%m-%d"),
        }
    )
    filterList.append(
        {
            "fieldname": "datestock",
            "operator": "<=",
            "value": datestock_end.strftime("%Y-%m-%d"),
        }
    )
    bq_method.DeleteRowFromTable(bqtable, bqdataset, bqjsonservicefile, filterList)

    for datestock in daterange(datestock_start, datestock_end):
        logger.info(f"Получение остатков из 1С {datestock.strftime('%d-%m-%Y')}.")
        query.SetParameter(
            "ДатаОстатковНачало",
            v83.newObject(
                "Граница",
                v83.ValueFromStringInternal(
                    f'{{"D",{datestock.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y%m%d%H%M%S")}}}'
                ),
                v83.ВидГраницы.Включая,
            ),
        )
        query.SetParameter(
            "ДатаОстатков",
            v83.newObject(
                "Граница",
                v83.ValueFromStringInternal(
                    f'{{"D",{datestock.replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y%m%d%H%M%S")}}}'
                ),
                v83.ВидГраницы.Включая,
            ),
        )

        choose = query.execute().choose()

        liststock = []
        while choose.next():
            dict = {}
            dict["item"] = choose.item
            dict["articul"] = choose.articul
            dict["scl"] = choose.scl
            dict["item_group"] = choose.item_group
            dict["item_group_cost"] = choose.item_group_cost
            dict["item_type"] = choose.item_type
            dict["stock_start"] = choose.stock_start
            dict["stock_in"] = choose.stock_in
            dict["stock_out"] = choose.stock_out
            dict["stock_end"] = choose.stock_end
            dict["reserv"] = choose.reserv
            dict["delivering"] = choose.delivering
            dict["free_qty"] = choose.free_qty
            dict["ordered"] = choose.ordered
            dict["datestock"] = datestock.date().isoformat()
            dict["dateexport"] = datetime.date.today().isoformat()
            liststock.append(dict)
        csvfields = []
        csvfields.append({"item": "STRING"})
        csvfields.append({"articul": "STRING"})
        csvfields.append({"scl": "STRING"})
        csvfields.append({"item_group": "STRING"})
        csvfields.append({"item_group_cost": "STRING"})
        csvfields.append({"item_type": "STRING"})
        csvfields.append({"stock_start": "FLOAT"})
        csvfields.append({"stock_in": "FLOAT"})
        csvfields.append({"stock_out": "FLOAT"})
        csvfields.append({"stock_end": "FLOAT"})
        csvfields.append({"reserv": "FLOAT"})
        csvfields.append({"delivering": "FLOAT"})
        csvfields.append({"ordered": "FLOAT"})
        csvfields.append({"free_qty": "FLOAT"})
        csvfields.append({"datestock": "DATE"})
        csvfields.append({"dateexport": "DATE"})

        bq_method.export_js_to_bq(
            liststock, bqtable, bqjsonservicefile, bqdataset, logger, csvfields
        )


def get_query_fullstock():
    return """
    ВЫБРАТЬ
	СГруппированныйЗапрос.item.Наименование КАК item,
	СГруппированныйЗапрос.scl.Наименование КАК scl,
	СГруппированныйЗапрос.articul КАК articul,
	Представление(СГруппированныйЗапрос.item_group) КАК item_group,
	Представление(СГруппированныйЗапрос.item_group_cost) КАК item_group_cost,
	Представление(СГруппированныйЗапрос.item_type) КАК item_type,
	СУММА(СГруппированныйЗапрос.reserv) КАК reserv,
	СУММА(СГруппированныйЗапрос.deliverying) КАК delivering,
	СУММА(СГруппированныйЗапрос.ordered) КАК ordered,
	СУММА(СГруппированныйЗапрос.stock_end) КАК stock_end,
	СУММА(СГруппированныйЗапрос.stock_start) КАК stock_start,
	СУММА(СГруппированныйЗапрос.stock_in) КАК stock_in,
	СУММА(СГруппированныйЗапрос.stock_out) КАК stock_out,
	СУММА(СГруппированныйЗапрос.stock_end - СГруппированныйЗапрос.reserv) КАК free_qty
ИЗ
	(ВЫБРАТЬ
		ТоварыНаСкладахОстатки.Номенклатура КАК item,
		ТоварыНаСкладахОстатки.Склад КАК scl,
		ТоварыНаСкладахОстатки.Номенклатура.Артикул КАК articul,
		ТоварыНаСкладахОстатки.Номенклатура.НоменклатурнаяГруппа КАК item_group,
		ТоварыНаСкладахОстатки.Номенклатура.НоменклатурнаяГруппаЗатрат КАК item_group_cost,
		ТоварыНаСкладахОстатки.Номенклатура.ВидНоменклатуры КАК item_type,
		ТоварыНаСкладахОстатки.КоличествоНачальныйОстаток КАК stock_start,
		0 КАК reserv,
		0 КАК deliverying,
		0 КАК ordered,
		ТоварыНаСкладахОстатки.КоличествоКонечныйОстаток КАК stock_end,
		ТоварыНаСкладахОстатки.КоличествоПриход КАК stock_in,
		ТоварыНаСкладахОстатки.КоличествоРасход КАК stock_out
	ИЗ
		РегистрНакопления.ТоварыНаСкладах.ОстаткиИОбороты(&ДатаОстатковНачало, &ДатаОстатков, День, , Склад.Код = "000000001") КАК ТоварыНаСкладахОстатки

	ОБЪЕДИНИТЬ ВСЕ

	ВЫБРАТЬ
		ТоварыВРезервеНаСкладахОстатки.Номенклатура,
		ТоварыВРезервеНаСкладахОстатки.Склад,
		ТоварыВРезервеНаСкладахОстатки.Номенклатура.Артикул,
		ТоварыВРезервеНаСкладахОстатки.Номенклатура.НоменклатурнаяГруппа,
		ТоварыВРезервеНаСкладахОстатки.Номенклатура.НоменклатурнаяГруппаЗатрат,
		ТоварыВРезервеНаСкладахОстатки.Номенклатура.ВидНоменклатуры,
		0,
		ТоварыВРезервеНаСкладахОстатки.КоличествоОстаток,
		0,
		0,
		0,
		0,
		0
	ИЗ
		РегистрНакопления.ТоварыВРезервеНаСкладах.Остатки(&ДатаОстатков, Склад.Код = "000000001") КАК ТоварыВРезервеНаСкладахОстатки

	ОБЪЕДИНИТЬ ВСЕ

	ВЫБРАТЬ
		ТоварыКПолучениюНаСклады.Номенклатура,
		ТоварыКПолучениюНаСклады.Склад,
		ТоварыКПолучениюНаСклады.Номенклатура.Артикул,
		ТоварыКПолучениюНаСклады.Номенклатура.НоменклатурнаяГруппа,
		ТоварыКПолучениюНаСклады.Номенклатура.НоменклатурнаяГруппаЗатрат,
		ТоварыКПолучениюНаСклады.Номенклатура.ВидНоменклатуры,
		0,
		0,
		ТоварыКПолучениюНаСклады.КоличествоОстаток,
		0,
		0,
		0,
		0
	ИЗ
		РегистрНакопления.ТоварыКПолучениюНаСклады.Остатки(&ДатаОстатков, Склад.Код = "000000001") КАК ТоварыКПолучениюНаСклады

	ОБЪЕДИНИТЬ ВСЕ

	ВЫБРАТЬ
		ЗаказыПоставщикамОстатки.Номенклатура,
		Скл.Ссылка,
		ЗаказыПоставщикамОстатки.Номенклатура.Артикул,
		ЗаказыПоставщикамОстатки.Номенклатура.НоменклатурнаяГруппа,
		ЗаказыПоставщикамОстатки.Номенклатура.НоменклатурнаяГруппаЗатрат,
		ЗаказыПоставщикамОстатки.Номенклатура.ВидНоменклатуры,
		0,
		0,
		ЗаказыПоставщикамОстатки.КоличествоОстаток,
		0,
		0,
		0,
		0
	ИЗ
		РегистрНакопления.ЗаказыПоставщикам.Остатки(&ДатаОстатков, ) КАК ЗаказыПоставщикамОстатки
			ЛЕВОЕ СОЕДИНЕНИЕ Справочник.Склады КАК Скл
			ПО (Скл.Код = "000000001")) КАК СГруппированныйЗапрос

СГРУППИРОВАТЬ ПО
	СГруппированныйЗапрос.item,
	СГруппированныйЗапрос.item_group,
	СГруппированныйЗапрос.item_group_cost,
	СГруппированныйЗапрос.item_type,
	СГруппированныйЗапрос.scl,
	СГруппированныйЗапрос.articul
    """


def get_query_goods_for_reciving_stock():
    return """
Выбрать Период как period,Номенклатура.Наименование name,Номенклатура.Артикул articul,Номенклатура.Код code,
КоличествоПриход stock_in,КоличествоРасход stock_out из
РегистрНакопления.ТоварыКПолучениюНаСклады.Обороты(&period_start,&period_end,День,Склад.Код="вр0000002")
"""
