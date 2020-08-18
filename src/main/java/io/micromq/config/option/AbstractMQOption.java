package io.micromq.config.option;

import io.micromq.common.MQException;
import io.micromq.config.ConfigSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * @Desctiption
 * @Author wallace
 * @Date 2020/8/18
 */
public abstract class AbstractMQOption<O extends AbstractMQOption<O, T>, T> implements MQOption<T> {
	private String name;
	private String category;
	private String description;
	protected T defaultValue;
	protected T value;

	@Override
	public String getName(){
		return name;
	}

	@Override
	public String getCategory() {
		return category;
	}

	@Override
	public String getDescription(){
		return description;
	}

	public T value(){
		return value;
	}

	public O withName(String name){
		this.name = StringUtils.trim(name);
		Validate.notEmpty(this.name);

		return self();
	}

	public O withCategory(String category){
		this.category = StringUtils.trim(category);
		Validate.notEmpty(this.category);

		return self();
	}

	public O withDescription(String description){
		this.description = description;
		return self();
	}

	public O withDefaultValue(T value){
		this.defaultValue = value;
		return self();
	}

	public O withNoDefaultValue(){
		return self();
	}

	public O parse(ConfigSource source){
		value = getValue(source);
		check();

		return self();
	}

	O self(){
		return (O) this;
	}

	protected abstract T getValue(ConfigSource source);
	protected abstract void check();
}
