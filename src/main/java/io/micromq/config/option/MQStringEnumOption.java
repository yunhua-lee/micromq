package io.micromq.config.option;

import org.apache.commons.configuration.Configuration;

/**
 * @Desctiption
 * @Author wallace
 * @Date 2020/8/19
 */
public class MQStringEnumOption extends MQEnumOption<String>{
	@Override
	protected void getValue(Configuration configuration) {
		value = configuration.getString(getName(), defaultValue);
	}
}
