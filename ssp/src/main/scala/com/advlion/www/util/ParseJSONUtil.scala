package com.advlion.www.util

import com.alibaba.fastjson.JSON

object ParseJSONUtil {
    /**
     * 解析ods.ods_dsp_resp_creative的json
     * @param jsonStr
     * @param flag
     * @return
     */
    def parseLogReturnImgUrl(jsonStr:String,flag:Int):String={
        if (jsonStr ==null)
            return null

        var imgURL : String = null;
        if(flag == 1){ //adslocation_type_id=3的,从img里面拿
            try {
                val jsonObject = JSON.parseObject(jsonStr)
                imgURL = jsonObject.getJSONArray("img").getJSONObject(0).getString("url")
            }catch{
                case ex:Exception =>
            }
        }else{//其他,直接从外面imgurl里面拿
            try {
                val jsonObject = JSON.parseObject(jsonStr)
                imgURL = jsonObject.getString("imgurl")
            }catch{
                case ex:Exception =>
            }
        }

        imgURL
    }

    def main(args: Array[String]): Unit = {
        val json3="{\"desc\":\"小学四年级英语\",\"ldp\":\"https:\\/\\/wolong-dsp.sm.cn\\/click?b=AKAAAEsAAABI25qYeGDSCadSfu6e9Jw7QoBNj5WIetDwxXokC7tFax3erbmnnhZruzJftJw5oBqogK055MihF7V8ybxdg2FgFYOtQn2Iavc_fk4:\",\"title\":\"小学四年级英语\",\"img\":[{\"url\":\"http:\\/\\/image-ad.sm.cn\\/admaterial\\/image\\/image_22466243_cc6cf057f1dc8db7b9bf13446cfe783ee15b5b75.jpg\",\"h\":320,\"w\":640}]}"


        println(parseLogReturnImgUrl(json3,1))

        val jsonOther = "{\"deeplink\":\"openapp.jdmobile:\\/\\/virtual?params=%7B%22SE%22%3A%22ADC_FcEysMwTmUCH8n9pZRlhA3PCQVrL7govCYLZVXGa%2Bnxs7lAhG8w3ZyyWVJhEdtTVuTffY03Uq1FJ%2Bg2XMYrQKwkoNc8eFdGRvAlV7hkUTdER87ugtNNtr97%2FfG6PUDVhjSw1JgbUflTDEdEXbSTIGBsd1pNTBZryVCJ7u7GChuI%3D%22%2C%22action%22%3A%22to%22%2C%22category%22%3A%22jump%22%2C%22des%22%3A%22getCoupon%22%2C%22ext%22%3A%22%7B%5C%22ad%5C%22%3A%5C%22%5C%22%2C%5C%22ch%5C%22%3A%5C%22%5C%22%2C%5C%22shop%5C%22%3A%5C%22%5C%22%2C%5C%22sku%5C%22%3A%5C%22%5C%22%2C%5C%22ts%5C%22%3A%5C%22%5C%22%2C%5C%22uniqid%5C%22%3A%5C%22%7B%5C%5C%5C%22material_id%5C%5C%5C%22%3A%5C%5C%5C%222275348482%5C%5C%5C%22%2C%5C%5C%5C%22pos_id%5C%5C%5C%22%3A%5C%5C%5C%224609%5C%5C%5C%22%2C%5C%5C%5C%22sid%5C%5C%5C%22%3A%5C%5C%5C%22274_40105e8ad5af4c598aa01f5974d5f81b_1%5C%5C%5C%22%7D%5C%22%7D%22%2C%22kepler_param%22%3A%7B%22channel%22%3A%222e3b9ecfb3a1465badbbbeb48df4140c%22%2C%22source%22%3A%22kepler-open%22%7D%2C%22m_param%22%3A%7B%22jdv%22%3A%22238571484%7Cvipliangmei%7Ct_1001802371_2275348482_4609%7Cadrealizable%7C_2_app_0_5cde41529b0e489a909d19b0b1b30003-p_4609%22%7D%2C%22sourceType%22%3A%22adx%22%2C%22sourceValue%22%3A%22vipliangmei_274%22%2C%22url%22%3A%22https%3A%2F%2Fccc-x.jd.com%2Fdsp%2Fnc%3Fext%3DaHR0cHM6Ly9wcm8ubS5qZC5jb20vbWFsbC9hY3RpdmUvM2VjWlFjTG5hVFVvYzRBUmdCQnQ3cG9ERWd5TS9pbmRleC5odG1sP1BUQUc9MTcwNTEuOS4xJmFkX29kPTE%26log%3DJZw8-as1WBw56fqZIVmfOdm8m5HaM9NMgbhjJB6IijBS04jthEWBBtCCcoLi4YCmx2nZKeV4pOfuA1GGDbq8-lKAUTTg1coI-yO92im54iWZYsp9_XdU_9nanKvnTAgPXnUCuTdaMvHV3OQ1F0WBTDpcQmJCdc4FMFFzYaHy2gbB33lqnOda4zJhjKiqmElzx3yVlIOlnw88ahHPj-23HpwgMfHTarlSdD5w3Py8nct11flRUxSfYr1sFaw30tIgNmlfhC3MlxCrC1ki9QROqb7Di2buyHIRlPz8QZ2Cle7rxwHK6kPySrxK3_6haahdtDIXta6g1Dt8txrewPaaJb8oOLil21eUvVHR2iqIldOgm1vsgShuavnqh-V2iYPkfI5aikE2-5W4fgXbOLaXsRXh-SkX296XyL6nGcZU84khLVuaav_inX1hHi90aUiipqIKQPTf_9KeP0HNY9UHNJTgTMsjwXHUK4js5iD4lW4ddRSyZizWbRZogKaR8mc7HvvlfbuEKL3JUK-Hdnp8K3QPNOkhNEum3BbwWJkVfKlaXPoqaDdgiExWJatyjRR4AfMwvYBxu6TSdSCOgjmG3Vkk6V2lhRK4AO3pP8T-0SqDddm3gH8IwH8_eDvLGx7I3Fj4Y6eB3QmLSLT5y5UogQ%26v%3D404%26SE%3D1%22%7D%0A\",\"imgurl\":\"https:\\/\\/img1.360buyimg.com\\/pop\\/jfs\\/t1\\/145021\\/5\\/914\\/74243\\/5eead174Ee6fcb8ae\\/4620cf195dcaef53.jpg\",\"interact_type\":0,\"ldp\":\"https:\\/\\/ccc-x.jd.com\\/dsp\\/nc?ext=aHR0cHM6Ly9wcm8ubS5qZC5jb20vbWFsbC9hY3RpdmUvM2VjWlFjTG5hVFVvYzRBUmdCQnQ3cG9ERWd5TS9pbmRleC5odG1sP1BUQUc9MTcwNTEuOS4xJmFkX29kPTE&log=JZw8-as1WBw56fqZIVmfOdm8m5HaM9NMgbhjJB6IijBS04jthEWBBtCCcoLi4YCmx2nZKeV4pOfuA1GGDbq8-lKAUTTg1coI-yO92im54iWZYsp9_XdU_9nanKvnTAgPXnUCuTdaMvHV3OQ1F0WBTDpcQmJCdc4FMFFzYaHy2gbB33lqnOda4zJhjKiqmElzx3yVlIOlnw88ahHPj-23HgxasJLxw02rOiWXLnTTHknUbJk-INrrAR0HLgJBVjXXwcs2gQ6gXEm2vW9lBPpDC5GSKBGmtKpXSjQScFlCMVvR7Ebu7JhITdIilXnGawZG1vw9Xpols1q74VXbeg1Wr86xJAlYwg-53T5-svWoFmITODDE1dNhJ8JsLc2paUQ7VjeLxSc19ehRc2G5XXjDADgI11YzeLRhjcFH4y66J6BFgyBwz4EEGEJHJMIZ8Iudi9wg2ZqeQguqU4DiIEtrJo1n8jHXmusBeBdnLvYf1RodSKLnbJzNetS-jwXbzHLwBrM_nvoZ_q3aKI3-2mZvslDj7BwpUXwolN6yX0FXCYoWIMkR46SoVk1ysIBo2cxP889q9w1JddMJG9BZNYLnwMEqiFffitHhO0biwkoaQBY&v=404\"}"
        println(parseLogReturnImgUrl(jsonOther,0))
    }
}
