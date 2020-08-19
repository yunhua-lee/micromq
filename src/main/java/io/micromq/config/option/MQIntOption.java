package io.micromq.config.option;

import io.micromq.common.MQCode;
import io.micromq.common.MQRuntimeException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.Validate;


/**
 * @Desctiption
 * @Author wallace
 * @Date 2020/8/18
 */
public class MQIntOption extends AbstractMQOption<MQIntOption, Integer> {
	private int min;
	private int max;

	public MQIntOption withMinValue(int min){
		this.min = min;
		return this;
	}

	public MQIntOption withMaxValue(int max){
		Validate.isTrue(max > min);
		this.max = max;
		return this;
	}

	@Override
	protected void getValue(Configuration configuration) {
		value = configuration.getInt(getName(), defaultValue);
	}

	@Override
	public void check() {
		if( value < min || value > max ){
			throw new MQRuntimeException(MQCode.INVALID_CONFIG, buildCheckMessage());
		}
	}

	private String buildCheckMessage(){
		return "Invalid value: " + value + ", valid range is [" + min + "," + max +"]";
	}
}
