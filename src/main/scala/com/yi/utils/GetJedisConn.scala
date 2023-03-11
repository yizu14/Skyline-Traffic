package com.yi.utils

import redis.clients.jedis.JedisPool

object GetJedisConn {

  private lazy val pool = new JedisPool
  def getJedis(index:Int=0) = {
    //    获取连接
    val resource = pool.getResource
    //    选择库
    resource.select(index)
    //    返回工具
    resource
  }

}
