package io.micromq.config.option;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.Validate;

/**
 * @Desctiption
 * @Author wallace
 * @Date 2020/8/18
 */
public class MQStringOption extends AbstractMQOption<MQStringOption, String> {

	@Override
	protected void getValue(Configuration configuration) {
		value = configuration.getString(getName(), defaultValue);
	}

	@Override
	protected void check() {
		Validate.notEmpty(value, "Option " + getName() + " can not be empty!");
	}
}
