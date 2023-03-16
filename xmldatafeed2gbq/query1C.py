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
		0,
		ЗаказыПоставщикамОстатки.КоличествоОстаток,
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


def get_query_itemref():
    return """
ВЫБРАТЬ
	СправочникНоменклатуры.Ссылка.Наименование КАК finished_product,
	СправочникНоменклатуры.Артикул КАК article,
	СправочникНоменклатуры.НоменклатурнаяГруппа.Представление КАК nom_group,
	ВЫРАЗИТЬ(ЗначенияАктуальность.Значение КАК Справочник.ЗначенияСвойствОбъектов).Представление КАК actuality,
	ВЫРАЗИТЬ(ЗначенияГруппаТкани.Значение КАК Справочник.ЗначенияСвойствОбъектов).Представление КАК textile_group,
	ВЫРАЗИТЬ(ЗначенияНомерТкани.Значение КАК Справочник.ЗначенияСвойствОбъектов).Представление КАК textile_n,
	ВЫРАЗИТЬ(ЗначенияПринт.Значение КАК Справочник.ЗначенияСвойствОбъектов).Представление КАК print_color,
	ВЫРАЗИТЬ(ЗначенияРАЗМЕР.Значение КАК Справочник.ЗначенияСвойствОбъектов).Представление КАК size,
	ВЫРАЗИТЬ(ЗначенияСтатусИзделия.Значение КАК Справочник.ЗначенияСвойствОбъектов).Представление КАК product_status,
	ВЫРАЗИТЬ(ЗначенияТГ.Значение КАК Справочник.ЗначенияСвойствОбъектов).Представление КАК product_group,
	ВЫРАЗИТЬ(ЗначенияТипТкани.Значение КАК Справочник.ЗначенияСвойствОбъектов).Представление КАК fabric_type,
	ВЫРАЗИТЬ(ЗначенияТипТкани.Значение КАК Справочник.ЗначенияСвойствОбъектов).Представление КАК strategy_status,
	ВЫРАЗИТЬ(ЗначенияТипТовара.Значение КАК Справочник.ЗначенияСвойствОбъектов).Представление КАК item,
	ВЫРАЗИТЬ(ЗначенияТкань.Значение КАК Справочник.ЗначенияСвойствОбъектов).Представление КАК textile,
	ВЫРАЗИТЬ(ЗначенияФорма.Значение КАК Справочник.ЗначенияСвойствОбъектов).Представление КАК form,
	СопоставлениеАртикулов.Организация.Наименование КАК Organisation,
	СопоставлениеАртикулов.Контрагент.Наименование КАК Counteragent,
	СопоставлениеАртикулов.Подразделение.Наименование КАК Department,
	СопоставлениеАртикулов.НовыйАртикул КАК NewArticle,
	СопоставлениеАртикулов.ДопШтрихкод КАК Ad_barcode,
	СопоставлениеАртикулов.id_товара_МП КАК id_product_marketplace,
	СопоставлениеАртикулов.НаменованиеТовараНаМП КАК name_marketplace,
	СопоставлениеАртикулов.АртикулWB КАК article_wb,
	Представление(СправочникНоменклатуры.ВидНоменклатуры) как item_type


ИЗ
	Справочник.Номенклатура КАК СправочникНоменклатуры
		ЛЕВОЕ СОЕДИНЕНИЕ РегистрСведений.СопоставлениеАртикулов КАК СопоставлениеАртикулов
		ПО (СопоставлениеАртикулов.Номенклатура = СправочникНоменклатуры.Ссылка)
		ЛЕВОЕ СОЕДИНЕНИЕ РегистрСведений.ЗначенияСвойствОбъектов КАК ЗначенияАктуальность
		ПО (ЗначенияАктуальность.Объект = СправочникНоменклатуры.Ссылка)
			И (ЗначенияАктуальность.Свойство = &Актуальность)
		ЛЕВОЕ СОЕДИНЕНИЕ РегистрСведений.ЗначенияСвойствОбъектов КАК ЗначенияГруппаТкани
		ПО (ЗначенияГруппаТкани.Объект = СправочникНоменклатуры.Ссылка)
			И (ЗначенияГруппаТкани.Свойство = &ГруппаТкани)
		ЛЕВОЕ СОЕДИНЕНИЕ РегистрСведений.ЗначенияСвойствОбъектов КАК ЗначенияНомерТкани
		ПО (ЗначенияНомерТкани.Объект = СправочникНоменклатуры.Ссылка)
			И (ЗначенияНомерТкани.Свойство = &НомерТкани)
		ЛЕВОЕ СОЕДИНЕНИЕ РегистрСведений.ЗначенияСвойствОбъектов КАК ЗначенияПринт
		ПО (ЗначенияПринт.Объект = СправочникНоменклатуры.Ссылка)
			И (ЗначенияПринт.Свойство = &Принт)
		ЛЕВОЕ СОЕДИНЕНИЕ РегистрСведений.ЗначенияСвойствОбъектов КАК ЗначенияРАЗМЕР
		ПО (ЗначенияРАЗМЕР.Объект = СправочникНоменклатуры.Ссылка)
			И (ЗначенияРАЗМЕР.Свойство = &РАЗМЕР)
		ЛЕВОЕ СОЕДИНЕНИЕ РегистрСведений.ЗначенияСвойствОбъектов КАК ЗначенияСтатусИзделия
		ПО (ЗначенияСтатусИзделия.Объект = СправочникНоменклатуры.Ссылка)
			И (ЗначенияСтатусИзделия.Свойство = &СтатусИзделия)
		ЛЕВОЕ СОЕДИНЕНИЕ РегистрСведений.ЗначенияСвойствОбъектов КАК ЗначенияТГ
		ПО (ЗначенияТГ.Объект = СправочникНоменклатуры.Ссылка)
			И (ЗначенияТГ.Свойство = &ТГ)
		ЛЕВОЕ СОЕДИНЕНИЕ РегистрСведений.ЗначенияСвойствОбъектов КАК ЗначенияТипТкани
		ПО (ЗначенияТипТкани.Объект = СправочникНоменклатуры.Ссылка)
			И (ЗначенияТипТкани.Свойство = &ТипТкани)
		ЛЕВОЕ СОЕДИНЕНИЕ РегистрСведений.ЗначенияСвойствОбъектов КАК ЗначенияТипТовара
		ПО (ЗначенияТипТовара.Объект = СправочникНоменклатуры.Ссылка)
			И (ЗначенияТипТовара.Свойство = &ТипТовара)
		ЛЕВОЕ СОЕДИНЕНИЕ РегистрСведений.ЗначенияСвойствОбъектов КАК ЗначенияТкань
		ПО (ЗначенияТкань.Объект = СправочникНоменклатуры.Ссылка)
			И (ЗначенияТкань.Свойство = &Ткань)
		ЛЕВОЕ СОЕДИНЕНИЕ РегистрСведений.ЗначенияСвойствОбъектов КАК ЗначенияФорма
		ПО (ЗначенияФорма.Объект = СправочникНоменклатуры.Ссылка)
			И (ЗначенияФорма.Свойство = &Форма)
ГДЕ
	ИСТИНА
	И НЕ СправочникНоменклатуры.ЭтоГруппа
	И НЕ СправочникНоменклатуры.ПометкаУдаления
"""


def get_query_price_changes():
    return """
ВЫБРАТЬ Регистратор doc_ref,
	Номенклатура.Наименование КАК finished_product,
	Номенклатура.Артикул КАК article, ТипЦен.Наименование price_name,ТипЦен.Код price_code, Цена как price, Период как date_price
	ИЗ РегистрСведений.ЦеныНоменклатуры

ГДЕ
	ИСТИНА
	и Регистратор в(
	Выбрать ссылка из документ.УстановкаЦенНоменклатуры.Изменения где Узел=&Узел
	)
"""


def get_query_stocks_for_marketplace():
    return """
ВЫБРАТЬ
	СопоставлениеАртикулов.id_товара_МП КАК id_sku,
	isnull(ОстаткиДляМаркетПлейс.ВыгружаемыйОстаток,0) КАК stock
ИЗ
	РегистрСведений.СопоставлениеАртикулов КАК СопоставлениеАртикулов
		левое СОЕДИНЕНИЕ РегистрСведений.ОстаткиДляМаркетПлейс КАК ОстаткиДляМаркетПлейс
		ПО СопоставлениеАртикулов.Номенклатура = ОстаткиДляМаркетПлейс.Номенклатура
ГДЕ
	СопоставлениеАртикулов.Организация.Код = &ОрганизацияКод
	И СопоставлениеАртикулов.ВыгружатьОстатки
	И СопоставлениеАртикулов.Контрагент.Код = &КонтрагентКод
	и ОстаткиДляМаркетПлейс.СкладОтгрузки.Код="000000001"
	"""


def get_query_order_history_status():
    return """
ВЫБРАТЬ
	ИсторияСменыСтатусовЗаказов.Дата КАК status_date,
	Представление(ИсторияСменыСтатусовЗаказов.Статус) КАК status_name,
	Представление(ИсторияСменыСтатусовЗаказов.Ответственный) КАК updated_by,
	ИсторияСменыСтатусовЗаказов.Заказ.Номер КАК order_number,
	ИсторияСменыСтатусовЗаказов.Заказ.Дата КАК order_date
ИЗ
	РегистрСведений.ИсторияСменыСтатусовЗаказов КАК ИсторияСменыСтатусовЗаказов
Где 1 = 1
"""


def get_query_documents_of_service_receipt():
    return """
    ВЫБРАТЬ
	ПоступлениеУслуг.Ссылка.Дата КАК date_doc,
	ПоступлениеУслуг.Ссылка.Номер КАК number_doc,
	"Поступление" КАК Doc_type,
	ПоступлениеУслуг.Ссылка.ДатаВходящегоДокумента КАК date_incoming,
	ПоступлениеУслуг.Ссылка.НомерВходящегоДокумента КАК number_incoming,
	Представление(ПоступлениеУслуг.Подразделение) КАК Department,
	Представление(ПоступлениеУслуг.Ссылка.Организация) КАК Org,
	Представление(ПоступлениеУслуг.Ссылка.Контрагент) КАК Customer,
	ПоступлениеУслуг.Ссылка.Контрагент.Код КАК Customer_cod,
	Представление(ПоступлениеУслуг.СтатьяЗатрат) КАК Сost_item,
	ПоступлениеУслуг.СтатьяЗатрат.Код КАК Сost_item_cod,
	Представление(ПоступлениеУслуг.Номенклатура) КАК Description_nom,
	ПоступлениеУслуг.Номенклатура.Код КАК Code_nom,
	ВЫРАЗИТЬ(ПоступлениеУслуг.Сумма КАК ЧИСЛО(15, 2)) КАК Amount,
	ПоступлениеУслуг.Количество КАК Quantity
ИЗ
	Документ.ПоступлениеТоваровУслуг.Услуги КАК ПоступлениеУслуг
ГДЕ Истина
	И ПоступлениеУслуг.Ссылка.Проведен
	И ПоступлениеУслуг.Ссылка.Дата Между &НачалоПериода и &КонецПериода
	И ПоступлениеУслуг.Ссылка.Контрагент.Код В ("000018783", "000065945", "000034615")

"""


def get_query_documents_of_comission_report():
    return """
ВЫБРАТЬ
	ОтчетКомиссионераОПродажахТовары.Ссылка.Номер КАК NumberReport,
	ОтчетКомиссионераОПродажахТовары.Ссылка.Дата КАК DateReport,
	ОтчетКомиссионераОПродажахТовары.Номенклатура.Артикул КАК ArticulNom,
	ОтчетКомиссионераОПродажахТовары.Номенклатура.Наименование КАК DescriptionNom,
	ОтчетКомиссионераОПродажахТовары.Номенклатура.Код КАК CodeNom,
	ОтчетКомиссионераОПродажахТовары.Цена КАК Price,
	ОтчетКомиссионераОПродажахТовары.Количество КАК Qty,
	ОтчетКомиссионераОПродажахТовары.Сумма КАК Sum,
	ОтчетКомиссионераОПродажахТовары.СуммаВознаграждения КАК SumFee,
	ОтчетКомиссионераОПродажахТовары.Заказ1С.Номер КАК NumberOrder,
	ОтчетКомиссионераОПродажахТовары.Заказ1С.Дата КАК DateOrder,
	ОтчетКомиссионераОПродажахТовары.Реализация.Номер КАК NumberSale,
	ОтчетКомиссионераОПродажахТовары.Реализация.Дата КАК DateSale,
	ОтчетКомиссионераОПродажахТовары.IDТовара КАК IdNom,
	ОтчетКомиссионераОПродажахТовары.Проект.Наименование КАК Project,ОтчетКомиссионераОПродажахТовары.Ссылка.Подразделение.Наименование КАК Department
ИЗ
	Документ.ОтчетКомиссионераОПродажах.Товары КАК ОтчетКомиссионераОПродажахТовары
	Где  Год(ОтчетКомиссионераОПродажахТовары.Ссылка.Дата)=2023
"""
