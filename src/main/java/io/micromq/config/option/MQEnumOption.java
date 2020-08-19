package io.micromq.config.option;

import io.micromq.common.MQCode;
import io.micromq.common.MQRuntimeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.Set;

/**
 * @Desctiption
 * @Author wallace
 * @Date 2020/8/19
 */
public abstract class MQEnumOption<T>  extends AbstractMQOption<MQEnumOption<T>, T>{
	private Set<T> validValues;

	public MQEnumOption<T> withValidValues(Set<T> validValues){
		Validate.notEmpty(validValues);
		this.validValues = validValues;

		return this;
	}

	@Override
	protected void check() {
		if( !validValues.contains(value)){
			throw new MQRuntimeException(MQCode.INVALID_CONFIG, buildCheckMessage());
		}
	}

	private String buildCheckMessage(){
		return "Invalid value: " + value + " for config: " + getName() +
				", valid value set are: " + StringUtils.join(validValues, ",");
	}
}
