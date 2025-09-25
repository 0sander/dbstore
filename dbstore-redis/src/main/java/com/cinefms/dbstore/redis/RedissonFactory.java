package com.cinefms.dbstore.redis;

import com.cinefms.dbstore.redis.util.LenientJsonCodec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.redisson.codec.JsonJacksonCodec;
import org.springframework.beans.factory.FactoryBean;

public class RedissonFactory implements FactoryBean<RedissonClient> {

	private static Log log = LogFactory.getLog(RedissonFactory.class);

	private boolean singleton = false;
	private String singleServer;
	private String auth;
	private JsonJacksonCodec codec = new LenientJsonCodec();

	public RedissonClient getObject() {
		log.info("### REDISSON FACTORY - CREATING ... ");
		Config config = new Config();
		SingleServerConfig ssc = config.useSingleServer().setAddress(singleServer).setRetryAttempts(3);
		if (auth != null) {
			ssc.setPassword(auth);
		}
		config.setCodec(codec);
		return org.redisson.Redisson.create(config);
	}

	public Class<?> getObjectType() {
		return RedissonClient.class;
	}

	@Override
	public boolean isSingleton() {
		return this.singleton;
	}

	public void setSingleton(boolean singleton) {
		this.singleton = singleton;
	}

	public String getSingleServer() {
		return singleServer;
	}

	public void setSingleServer(String singleServer) {
		this.singleServer = singleServer;
	}

	public String getAuth() {
		return auth;
	}

	public void setAuth(String auth) {
		this.auth = auth;
	}

	public JsonJacksonCodec getCodec() {
		return codec;
	}

	public void setCodec(JsonJacksonCodec codec) {
		this.codec = codec;
	}

}
