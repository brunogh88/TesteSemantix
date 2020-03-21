from pyspark import SparkConf, SparkContext
from operator import add

try:
    #configuração
    v_configuracao = (SparkConf()
                      .setMaster("local")
                      .setAppName("TestePython")
                      .set("spark.executor.memory", "5g"))
    v_contex = SparkContext(conf=v_configuracao)

    print("Começando.....")
    #Leitura dos dados
    v_dados_julho = v_contex.textFile('NASA_access_log_Jul95.gz')
    v_dados_julho = v_dados_julho.cache()

    v_dados_agosto = v_contex.textFile('NASA_access_log_Aug95.gz')
    v_dados_agosto = v_dados_agosto.cache()

    #Quantidade de hosts unicos
    v_host_unicos_qtde_julho = v_dados_julho.flatMap(lambda x: x.split(' ')[0]).distinct().count()
    v_host_unicos_qtde_agosto = v_dados_agosto.flatMap(lambda x: x.split(' ')[0]).distinct().count()

    print('Quantidade de Hosts únicos em Julho: %s' % v_host_unicos_qtde_julho)
    print('Quantidade de Hosts únicos em Agosto: %s' % v_host_unicos_qtde_agosto)

    #quantidade de code 404
    v_code_404_julho = v_dados_julho.filter(lambda x: x.split(' ')[-2] == '404').cache()
    v_code_404_agosto = v_dados_agosto.filter(lambda x: x.split(' ')[-2] == '404').cache()

    print('Quantidade de erros 404 em Julho: %s' % v_code_404_julho.count())
    print('Quantidade de erros 404 em Agosto %s' % v_code_404_agosto.count())


    #As 5 URL's que mais causaram erro 404
    v_5_urls_code_erro_404_mais_frequentes_julho = v_code_404_julho.flatMap(lambda x: x.split('"')[1].split('')[1])\
        .flatMap(lambda x: (x, 1))\
        .reduceByKey(lambda x,y:x+y)\
        .sortBy(lambda x: -x).take(5)

    v_5_urls_code_erro_404_mais_frequentes_agosto = v_code_404_agosto.flatMap(lambda x: x.split('"')[1].split('')[1])\
        .flatMap(lambda x: (x, 1))\
        .reduceByKey(lambda x, y: x + y)\
        .sortBy(lambda x: -x).take(5)

    #Quantidade de code 404 por dia
    v_qunatidade_dia_code_404_julho = v_code_404_julho.flatMap(lambda x: x.split('[').split(':'))\
        .flatMap(lambda x: (x, 1))\
        .reduceByKey(lambda x,y: x+y).collect()

    v_qunatidade_dia_code_404_agosto = v_code_404_agosto.flatMap(lambda x: x.split('[').split(':')) \
        .flatMap(lambda x: (x, 1)) \
        .reduceByKey(lambda x, y: x + y).collect()

    print('Quantidade de code 404 por dia em julho: %s' % v_qunatidade_dia_code_404_julho)
    print('Quantidade de code 404 por dia em agosto: %s' % v_qunatidade_dia_code_404_agosto)


    #total de bytes retornados
    v_quantidade_bytes_julho = v_dados_julho.flatMap(lambda x: x.split(' ')[-1]).sum()
    v_quantidade_bytes_agosto = v_dados_agosto.flatMap(lambda x: x.split(' ')[-1]).sum()

    print('Quantidade de bytes retornados em julho: %s' % v_quantidade_bytes_julho)
    print('Quantidade de cbytes retornados em agosto: %s' % v_quantidade_bytes_agosto)

    v_contex.stop()
except AttributeError:
    print('Ocorreu um erro. O processo parou....')
    v_contex.stop()



