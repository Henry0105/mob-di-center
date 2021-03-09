package com.youzu.mob.contacts

import com.youzu.mob.utils.Constants._

import java.util.Date
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.BaseAnalysis
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PhoneContactsWordSplit {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(
        s"""
           |error number of input parameters,please check your input
           |parameters like:<day>
         """.stripMargin)
      System.exit(-1)
    }
    println(args.mkString(","))
    println(s"start time: ${new Date()}")
    val day = args(0)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val UDFRetrieve1 = udf {
      v: SparseVector => v.indices
    }
    val UDFRetrieve2 = udf {
      v: SparseVector => v.values
    }

    val DataOrigin = spark.sql(
      s"""
         |select phone,words_list
         |from $DM_MOBDI_TMP.phone_contacts_index_word_split_prepare
         |where day='$day'
      """.stripMargin)

    val testData = DataOrigin.withColumn("words_list", concat_ws(" ", col("words_list")))

    // stop words分词时去除标点符号和百家姓
    val filter = new StopRecognition()
    filter.insertStopNatures("w")
    val stopWords = List(" ", "", "／", "·", "；", "＂", "．", "％", "：", "］", "［", "－", "\"\"")
    stopWords.foreach((a: String) => filter.insertStopWords(a))
    surnames.foreach((a: String) => filter.insertStopWords(a))

    // 分词
    val UDFwords = udf { x: String =>
      BaseAnalysis.parse(x).recognition(filter).toStringWithOutNature("/").split("/")
    }
    val temp2 = testData.withColumn("splitwords", UDFwords(col("words_list")))
    // 把空行删了
    val temp3 = temp2.where("splitwords != Array(\"\")")
    println(s"分词time: ${new Date()}")

    // 用CountVectorizer对分词结果进行计数，保留出现最多的前2000个词，记录每个phone所出现的分词index
    val cvModel = CountVectorizerModel.load("/dmgroup/dba/modelpath/20191128/phone_index/cvModel11")
    val cvDf = cvModel.transform(temp3)
    val idf = new IDF().setInputCol("feature").setOutputCol("features")
    val idfModel = idf.fit(cvDf)
    val rescaledData = idfModel.transform(cvDf)
    val rescaledData2 = rescaledData
      .withColumn("word_index", UDFRetrieve1(col("features")))
      .withColumn("tfidf_score", UDFRetrieve2(col("features")))
    println(s"CountVectorizer time: ${new Date()}")

    rescaledData2.repartition(300).createOrReplaceTempView("output")
    spark.sql(
      s"""
         |insert overwrite table $DM_MOBDI_TMP.phone_contacts_index_word_split partition(day='$day')
         |select phone,word_index,tfidf_score
         |from output
       """.stripMargin)
    println(s"insert time: ${new Date()}")

    // 对分词结果，通过Word2Vec抽取50个词向量，记录每个phone的分词在这50个向量上的转换结果，并分别按4分位数分箱
    import spark.implicits._
    val temp4 = temp2.where(size($"splitwords") > 1)
    val model_2 = Word2VecModel.load("/dmgroup/dba/modelpath/20191128/phone_index/word2vec11")
    val result_2 = model_2.transform(temp4)
    println(s"Word2Vec time: ${new Date()}")

    val toArr: Any => Array[Double] = _.asInstanceOf[DenseVector].toArray
    val toArrUdf = udf(toArr)

    var result2 = result_2
      .withColumn("features_arr", toArrUdf(col("result")))
      .select(
        col("phone"),
        col("features_arr").getItem(0).as("vec1"),
        col("features_arr").getItem(1).as("vec2"),
        col("features_arr").getItem(2).as("vec3"),
        col("features_arr").getItem(3).as("vec4"),
        col("features_arr").getItem(4).as("vec5"),
        col("features_arr").getItem(5).as("vec6"),
        col("features_arr").getItem(6).as("vec7"),
        col("features_arr").getItem(7).as("vec8"),
        col("features_arr").getItem(8).as("vec9"),
        col("features_arr").getItem(9).as("vec10"),
        col("features_arr").getItem(10).as("vec11"),
        col("features_arr").getItem(11).as("vec12"),
        col("features_arr").getItem(12).as("vec13"),
        col("features_arr").getItem(13).as("vec14"),
        col("features_arr").getItem(14).as("vec15"),
        col("features_arr").getItem(15).as("vec16"),
        col("features_arr").getItem(16).as("vec17"),
        col("features_arr").getItem(17).as("vec18"),
        col("features_arr").getItem(18).as("vec19"),
        col("features_arr").getItem(19).as("vec20"),
        col("features_arr").getItem(20).as("vec21"),
        col("features_arr").getItem(21).as("vec22"),
        col("features_arr").getItem(22).as("vec23"),
        col("features_arr").getItem(23).as("vec24"),
        col("features_arr").getItem(24).as("vec25"),
        col("features_arr").getItem(25).as("vec26"),
        col("features_arr").getItem(26).as("vec27"),
        col("features_arr").getItem(27).as("vec28"),
        col("features_arr").getItem(28).as("vec29"),
        col("features_arr").getItem(29).as("vec30"),
        col("features_arr").getItem(30).as("vec31"),
        col("features_arr").getItem(31).as("vec32"),
        col("features_arr").getItem(32).as("vec33"),
        col("features_arr").getItem(33).as("vec34"),
        col("features_arr").getItem(34).as("vec35"),
        col("features_arr").getItem(35).as("vec36"),
        col("features_arr").getItem(36).as("vec37"),
        col("features_arr").getItem(37).as("vec38"),
        col("features_arr").getItem(38).as("vec39"),
        col("features_arr").getItem(39).as("vec40"),
        col("features_arr").getItem(40).as("vec41"),
        col("features_arr").getItem(41).as("vec42"),
        col("features_arr").getItem(42).as("vec43"),
        col("features_arr").getItem(43).as("vec44"),
        col("features_arr").getItem(44).as("vec45"),
        col("features_arr").getItem(45).as("vec46"),
        col("features_arr").getItem(46).as("vec47"),
        col("features_arr").getItem(47).as("vec48"),
        col("features_arr").getItem(48).as("vec49"),
        col("features_arr").getItem(49).as("vec50")
      )
    println(s"vec time: ${new Date()}")
    val inputCols = result2.columns.diff(Seq("phone"))
    val discretizer = new QuantileDiscretizer()
      .setInputCols(inputCols)
      .setOutputCols(inputCols.map(_ + "_quantile"))
      .setNumBuckets(4)
    result2 = discretizer.fit(result2).transform(result2).drop(inputCols.toSeq: _*)
    println(s"QuantileDiscretizer time: ${new Date()}")

    result2.repartition(400).createOrReplaceTempView("tmp_output")
    spark.sql(
      s"""
         |insert overwrite table $DM_MOBDI_TMP.phone_contacts_word2vec_index partition(day='$day')
         |select phone,
         |       vec1_quantile,
         |       vec2_quantile,
         |       vec3_quantile,
         |       vec4_quantile,
         |       vec5_quantile,
         |       vec6_quantile,
         |       vec7_quantile,
         |       vec8_quantile,
         |       vec9_quantile,
         |       vec10_quantile,
         |       vec11_quantile,
         |       vec12_quantile,
         |       vec13_quantile,
         |       vec14_quantile,
         |       vec15_quantile,
         |       vec16_quantile,
         |       vec17_quantile,
         |       vec18_quantile,
         |       vec19_quantile,
         |       vec20_quantile,
         |       vec21_quantile,
         |       vec22_quantile,
         |       vec23_quantile,
         |       vec24_quantile,
         |       vec25_quantile,
         |       vec26_quantile,
         |       vec27_quantile,
         |       vec28_quantile,
         |       vec29_quantile,
         |       vec30_quantile,
         |       vec31_quantile,
         |       vec32_quantile,
         |       vec33_quantile,
         |       vec34_quantile,
         |       vec35_quantile,
         |       vec36_quantile,
         |       vec37_quantile,
         |       vec38_quantile,
         |       vec39_quantile,
         |       vec40_quantile,
         |       vec41_quantile,
         |       vec42_quantile,
         |       vec43_quantile,
         |       vec44_quantile,
         |       vec45_quantile,
         |       vec46_quantile,
         |       vec47_quantile,
         |       vec48_quantile,
         |       vec49_quantile,
         |       vec50_quantile
         |from tmp_output
       """.stripMargin)
    println(s"over time: ${new Date()}")
  }

  // 百家姓
  private val surnames = List("赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈", "褚", "卫", "蒋", "沈", "韩",
    "杨", "朱", "秦", "尤", "许", "何", "吕", "施", "张", "孔", "曹", "严", "华", "金", "魏", "陶", "姜", "戚", "谢",
    "邹", "喻", "柏", "水", "窦", "章", "云", "苏", "潘", "葛", "奚", "范", "彭", "郎", "鲁", "韦", "昌", "马", "苗",
    "凤", "花", "方", "俞", "任", "袁", "柳", "酆", "鲍", "史", "唐", "费", "廉", "岑", "薛", "雷", "贺", "倪", "汤",
    "滕", "殷", "罗", "毕", "郝", "邬", "安", "常", "乐", "于", "时", "傅", "皮", "卞", "齐", "康", "伍", "余", "元",
    "卜", "顾", "孟", "平", "黄", "和", "穆", "萧", "尹", "姚", "邵", "湛", "汪", "祁", "毛", "禹", "狄", "米", "贝",
    "明", "臧", "计", "伏", "成", "戴", "谈", "宋", "茅", "庞", "熊", "纪", "舒", "屈", "项", "祝", "董", "梁", "杜",
    "阮", "蓝", "闵", "席", "季", "麻", "强", "贾", "路", "娄", "危", "江", "童", "颜", "郭", "梅", "盛", "林", "刁",
    "锺", "徐", "邱", "骆", "高", "夏", "蔡", "田", "樊", "胡", "凌", "霍", "虞", "万", "支", "柯", "昝", "管", "卢",
    "莫", "经", "房", "裘", "缪", "干", "解", "应", "宗", "丁", "宣", "贲", "邓", "郁", "单", "杭", "洪", "包", "诸",
    "左", "石", "崔", "吉", "钮", "龚", "程", "嵇", "邢", "滑", "裴", "陆", "荣", "翁", "荀", "羊", "於", "惠", "甄",
    "麴", "家", "封", "芮", "羿", "储", "靳", "汲", "邴", "糜", "松", "井", "段", "富", "巫", "乌", "焦", "巴", "弓",
    "牧", "隗", "山", "谷", "车", "侯", "宓", "蓬", "全", "郗", "班", "仰", "秋", "仲", "伊", "宫", "宁", "仇", "栾",
    "暴", "甘", "钭", "历", "戎", "祖", "武", "符", "刘", "景", "詹", "束", "龙", "叶", "幸", "司", "韶", "郜", "黎",
    "蓟", "溥", "印", "宿", "白", "怀", "蒲", "邰", "从", "鄂", "索", "咸", "籍", "赖", "卓", "蔺", "屠", "蒙", "池",
    "乔", "阳", "郁", "胥", "能", "苍", "双", "闻", "莘", "党", "翟", "谭", "贡", "劳", "逄", "姬", "申", "扶", "堵",
    "冉", "宰", "郦", "雍", "却", "璩", "桑", "桂", "濮", "牛", "寿", "通", "边", "扈", "燕", "冀", "僪", "浦", "尚",
    "农", "温", "别", "庄", "晏", "柴", "瞿", "阎", "充", "慕", "连", "茹", "习", "宦", "艾", "鱼", "容", "向", "古",
    "易", "慎", "戈", "廖", "庾", "终", "暨", "居", "衡", "步", "都", "耿", "满", "弘", "匡", "国", "文", "寇", "广",
    "禄", "阙", "东", "欧", "殳", "沃", "利", "蔚", "越", "夔", "隆", "师", "巩", "厍", "聂", "晁", "勾", "敖", "融",
    "冷", "訾", "辛", "阚", "那", "简", "饶", "空", "曾", "毋", "沙", "乜", "养", "鞠", "须", "丰", "巢", "关", "蒯",
    "相", "查", "后", "荆", "红", "游", "竺", "权", "逮", "盍", "益", "桓", "公", "召", "有", "舜", "丛", "岳", "寸",
    "贰", "皇", "侨", "彤", "竭", "端", "赫", "实", "甫", "集", "象", "翠", "狂", "辟", "典", "良", "函", "芒", "苦",
    "其", "京", "中", "夕", "之", "冠", "宾", "香", "果", "蹇", "称", "诺", "来", "多", "繁", "戊", "朴", "回", "毓",
    "税", "荤", "靖", "绪", "愈", "硕", "牢", "买", "但", "巧", "枚", "撒", "泰", "秘", "亥", "绍", "以", "壬", "森",
    "斋", "释", "奕", "姒", "朋", "求", "羽", "用", "占", "真", "穰", "翦", "闾", "漆", "贵", "代", "贯", "旁", "崇",
    "栋", "告", "休", "褒", "谏", "锐", "皋", "闳", "在", "歧", "禾", "示", "是", "委", "钊", "频", "嬴", "呼", "大",
    "威", "昂", "律", "冒", "保", "系", "抄", "定", "化", "莱", "校", "么", "抗", "祢", "綦", "悟", "宏", "功", "庚",
    "务", "敏", "捷", "拱", "兆", "丑", "丙", "畅", "苟", "随", "类", "卯", "俟", "友", "答", "乙", "允", "甲", "留",
    "尾", "佼", "玄", "乘", "裔", "延", "植", "环", "矫", "赛", "昔", "侍", "度", "旷", "遇", "偶", "前", "由", "咎",
    "塞", "敛", "受", "泷", "袭", "衅", "叔", "圣", "御", "夫", "仆", "镇", "藩", "邸", "府", "掌", "首", "员", "焉",
    "戏", "可", "智", "尔", "凭", "悉", "进", "笃", "厚", "仁", "业", "肇", "资", "合", "仍", "九", "衷", "哀", "刑",
    "俎", "仵", "圭", "夷", "罕", "洛", "淦", "洋", "邶", "郸", "徭", "蛮", "汗", "孛", "乾", "帖", "郯", "邗", "邛",
    "剑", "虢", "隋", "蒿", "茆", "菅", "苌", "树", "桐", "锁", "钟", "机", "盘", "铎", "斛", "玉", "线", "针", "箕",
    "庹", "绳", "磨", "蒉", "瓮", "弭", "刀", "疏", "牵", "浑", "恽", "势", "世", "仝", "同", "蚁", "止", "戢", "睢",
    "冼", "种", "涂", "肖", "己", "泣", "潜", "卷", "脱", "谬", "蹉", "赧", "浮", "顿", "说", "次", "错", "念", "夙",
    "斯", "完", "丹", "表", "聊", "源", "姓", "吾", "寻", "展", "出", "不", "户", "闭", "才", "无", "书", "学", "愚",
    "本", "性", "雪", "霜", "烟", "寒", "少", "字", "桥", "板", "斐", "独", "千", "诗", "嘉", "扬", "善", "揭", "祈",
    "析", "赤", "紫", "青", "柔", "刚", "奇", "拜", "佛", "陀", "弥", "阿", "素", "长", "僧", "隐", "仙", "隽", "宇",
    "祭", "酒", "淡", "塔", "琦", "闪", "始", "星", "南", "天", "接", "波", "碧", "速", "禚", "腾", "潮", "镜", "似",
    "澄", "潭", "謇", "纵", "渠", "奈", "风", "春", "濯", "沐", "茂", "英", "兰", "檀", "藤", "枝", "检", "生", "折",
    "登", "驹", "骑", "貊", "虎", "肥", "鹿", "雀", "野", "禽", "飞", "节", "宜", "鲜", "粟", "栗", "豆", "帛", "官",
    "布", "衣", "藏", "宝", "钞", "银", "门", "盈", "庆", "喜", "及", "普", "建", "营", "巨", "望", "希", "道", "载",
    "声", "漫", "犁", "力", "贸", "勤", "革", "改", "兴", "亓", "睦", "修", "信", "闽", "北", "守", "坚", "勇", "汉",
    "练", "尉", "士", "旅", "五", "令", "将", "旗", "军", "行", "奉", "敬", "恭", "仪", "母", "堂", "丘", "义", "礼",
    "慈", "孝", "理", "伦", "卿", "问", "永", "辉", "位", "让", "尧", "依", "犹", "介", "承", "市", "所", "苑", "杞",
    "剧", "第", "零", "谌", "招", "续", "达", "忻", "六", "鄞", "战", "迟", "候", "宛", "励", "粘", "萨", "邝", "覃",
    "辜", "初", "楼", "城", "区", "局", "台", "原", "考", "妫", "纳", "泉", "清", "德", "卑", "过", "麦", "曲", "竹",
    "百", "福", "言", "佟", "爱", "年", "笪", "谯", "哈", "墨", "赏", "伯", "佴", "佘", "牟", "商", "琴", "后", "况",
    "亢", "缑", "帅", "海", "归", "钦", "鄢", "汝", "法", "闫", "楚", "晋", "督", "仉", "盖", "逯", "库", "郏", "逢",
    "阴", "薄", "厉", "稽", "开", "光")

}
