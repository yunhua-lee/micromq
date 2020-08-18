package io.micromq.config.option;

import io.micromq.common.MQException;
import io.micromq.config.ConfigSource;
import org.apache.commons.lang3.Validate;

/**
 * @Desctiption
 * @Author wallace
 * @Date 2020/8/18
 */
public class MQStringOption extends AbstractMQOption<MQStringOption, String> {
	@Override
	protected String getValue(ConfigSource source) {
		if( defaultValue == null ) {
			return source.getConfig(getCategory()).getString(getName());
		}
		else {
			return source.getConfig(getCategory()).getString(getName(), defaultValue);
		}
	}

	@Override
	protected void check() {
		Validate.notEmpty(value, "Option " + getName() + " can not be empty!");
	}
}
