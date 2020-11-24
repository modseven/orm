<?php
/**
 * [Object Relational Mapping][ref-orm] (ORM) is a method of abstracting database
 * access to standard PHP calls. All table rows are represented as model objects,
 * with object properties representing row data. ORM in Modseven generally follows
 * the [Active Record][ref-act] pattern.
 * [ref-orm]: http://wikipedia.org/wiki/Object-relational_mapping
 * [ref-act]: http://wikipedia.org/wiki/Active_record
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\ORM;

use stdClass;
use Countable;
use serializable;
use JsonException;
use ReflectionMethod;
use ReflectionFunction;
use ReflectionException;

use Modseven\Arr;
use Modseven\Inflector;
use Modseven\Model;
use Modseven\Validation;

use Modseven\Database\DB;
use Modseven\Database\Result;
use Modseven\Database\Database;
use Modseven\Database\Query\Builder;

class ORM extends Model implements serializable
{
    /**
     * Stores column information for ORM models
     * @var array
     */
    protected static array $_column_cache = [];

    /**
     * Initialization storage for ORM models
     * @var array
     */
    protected static array $_init_cache = [];

    /**
     * "Has one" relationships
     * @var array
     */
    protected array $_has_one = [];

    /**
     * "Belongs to" relationships
     * @var array
     */
    protected array $_belongs_to = [];

    /**
     * "Has many" relationships
     * @var array
     */
    protected array $_has_many = [];

    /**
     * Relationships that should always be joined
     * @var array
     */
    protected array $_load_with = [];

    /**
     * Validation object created before saving/updating
     * @var Validation
     */
    protected ?Validation $_validation = null;

    /**
     * Current object
     * @var mixed
     */
    protected $_object = [];

    /**
     * Changed values
     * @var array
     */
    protected array $_changed = [];

    /**
     * Original Values
     * @var array
     */
    protected array $_original_values = [];

    /**
     * Related Values
     * @var array
     */
    protected array $_related = [];

    /**
     * Validated?
     * @var bool
     */
    protected bool $_valid = false;

    /**
     * Loaded?
     * @var bool
     */
    protected bool $_loaded = false;

    /**
     * Saved?
     * @var bool
     */
    protected bool $_saved = false;

    /**
     * Sorting parameter
     * @var array
     */
    protected array $_sorting;

    /**
     * Foreign key suffix
     * @var string
     */
    protected string $_foreign_key_suffix = '_id';

    /**
     * Model name
     * @var string|null
     */
    protected ?string $_object_name = null;

    /**
     * Plural model name
     * @var string|null
     */
    protected ?string $_object_plural = null;

    /**
     * Table name
     * @var string
     */
    protected string $_table_name;

    /**
     * Table columns
     * @var array
     */
    protected array $_table_columns;

    /**
     * Auto-update columns for updates
     * @var string
     */
    protected ?string $_updated_column = null;

    /**
     * Auto-update columns for creation
     * @var mixed
     */
    protected $_created_column = null;

    /**
     * Auto-serialize and unserialize columns on get/set
     * @var array
     */
    protected array $_serialize_columns = [];

    /**
     * Table primary key
     * @var string
     */
    protected string $_primary_key = 'id';

    /**
     * Primary key value
     * @var mixed
     */
    protected $_primary_key_value;

    /**
     * Model configuration, table names plural?
     * @var bool
     */
    protected bool $_table_names_plural = true;

    /**
     * Model configuration, reload on wakeup?
     * @var bool
     */
    protected bool $_reload_on_wakeup = true;

    /**
     * Database Object
     * @var Database
     */
    protected ?Database $_db = null;

    /**
     * Database config group
     * @var string
     */
    protected ?string $_db_group = null;

    /**
     * Database methods applied
     * @var array
     */
    protected array $_db_applied = [];

    /**
     * Database methods pending
     * @var array
     */
    protected array $_db_pending = [];

    /**
     * Reset builder
     * @var bool
     */
    protected bool $_db_reset = true;

    /**
     * Database query builder
     * @var Builder
     */
    protected ?Builder $_db_builder;

    /**
     * With calls already applied
     * @var array
     */
    protected array $_with_applied = [];

    /**
     * Data to be loaded into the model from a database call cast
     * @var array
     */
    protected array $_cast_data = [];

    /**
     * The message filename used for validation errors.
     * Defaults to ORM::$_object_name
     * @var string
     */
    protected ?string $_errors_filename = null;

    /**
     * List of behaviors
     * @var array
     */
    protected array $_behaviors = [];

    /**
     * List of private columns that will not appear in array or object
     * @var array
     */
    protected array $_private_columns = [];

    /**
     * Creates and returns a new model.
     *
     * @chainable
     *
     * @param string $model Model name
     * @param mixed  $id    Parameter for find()
     *
     * @return  Model
     */
    public static function factory(string $model, $id = null): Model
    {
        return new $model($id);
    }

    /**
     * Constructs a new model and loads a record if given
     *
     * @param mixed $id Parameter for find or object to load
     *
     * @throws \Modseven\Exception
     * @throws \Modseven\Database\Exception
     */
    public function __construct($id = null)
    {
        $this->_initialize();

        // Invoke all behaviors
        foreach ($this->_behaviors as $behavior)
        {
            if ($this->_loaded || ( ! $behavior->onConstruct($this, $id)))
            {
                return;
            }
        }

        if ($id !== null)
        {
            if (is_array($id))
            {
                foreach ($id as $column => $value)
                {
                    // Passing an array of column => values
                    $this->where($column, '=', $value);
                }

                $this->find();
            }
            else
            {
                // Passing the primary key
                $this->where($this->_object_name . '.' . $this->_primary_key, '=', $id)->find();
            }
        }
        elseif ( ! empty($this->_cast_data))
        {
            // Load preloaded data from a database call cast
            $this->_loadValues($this->_cast_data);

            $this->_cast_data = [];
        }
    }

    /**
     * Prepares the model database connection, determines the table name,
     * and loads column information.
     *
     * @throws \Modseven\Exception
     * @throws \Modseven\Database\Exception
     */
    protected function _initialize() : void
    {
        // Set the object name if none predefined
        if (empty($this->_object_name))
        {
            //$this->_object_name = strtolower(substr(get_class($this), 6));
            $class = get_class($this);
            $this->_object_name = strtolower(str_replace('\\', '_', substr($class, strpos($class, 'Model') + 6)));
        }

        // Check if this model has already been initialized
        if ( ! $init = Arr::get(static::$_init_cache, $this->_object_name, false))
        {
            $init = [
                '_belongs_to' => [],
                '_has_one'    => [],
                '_has_many'   => [],
            ];

            // Set the object plural name if none predefined
            if ( ! isset($this->_object_plural))
            {
                $init['_object_plural'] = Inflector::plural($this->_object_name);
            }

            if ( ! $this->_errors_filename)
            {
                $init['_errors_filename'] = $this->_object_name;
            }

            // Get database instance
            if ( ! is_object($this->_db))
            {
                $init['_db'] = Database::instance($this->_db_group);
            }

            if (empty($this->_table_name))
            {
                // Table name is the same as the object name
                $init['_table_name'] = $this->_object_name;

                if ($this->_table_names_plural === true)
                {
                    // Make the table name plural
                    $init['_table_name'] = Arr::get($init, '_object_plural', $this->_object_plural);
                }
            }

            $defaults = [];

            foreach ($this->_belongs_to as $alias => $details)
            {
                if ( ! isset($details['model']))
                {
                    $defaults['model'] = $alias;
                }

                $defaults['foreign_key'] = $alias . $this->_foreign_key_suffix;

                $init['_belongs_to'][$alias] = array_merge($defaults, $details);
            }

            foreach ($this->_has_one as $alias => $details)
            {
                if ( ! isset($details['model']))
                {
                    $defaults['model'] = $alias;
                }

                $defaults['foreign_key'] = $this->_object_name . $this->_foreign_key_suffix;

                $init['_has_one'][$alias] = array_merge($defaults, $details);
            }

            foreach ($this->_has_many as $alias => $details)
            {
                if ( ! isset($details['model']))
                {
                    $defaults['model'] = $alias;
                }

                $defaults['foreign_key'] = $this->_object_name . $this->_foreign_key_suffix;
                $defaults['through'] = null;

                if ( ! isset($details['far_key']))
                {
                    $defaults['far_key'] = Inflector::singular($alias) . $this->_foreign_key_suffix;
                }

                $init['_has_many'][$alias] = array_merge($defaults, $details);
            }

            static::$_init_cache[$this->_object_name] = $init;
        }

        // Assign initialized properties to the current object
        foreach ($init as $property => $value)
        {
            $this->{$property} = $value;
        }

        // Load column information
        $this->reloadColumns();

        // Clear initial model state
        $this->clear();

        // Create the behaviors classes
        foreach ($this->behaviors() as $behavior => $behavior_config)
        {
            $this->_behaviors[] = Behavior::factory($behavior, $behavior_config);
        }
    }

    /**
     * Initializes validation rules, and labels
     */
    protected function _validation() : void
    {
        // Build the validation object with its rules
        $this->_validation = Validation::factory($this->_object)
                                       ->bind(':model', $this)
                                       ->bind(':original_values', $this->_original_values)
                                       ->bind(':changed', $this->_changed);

        foreach ($this->rules() as $field => $rules)
        {
            $this->_validation->rules($field, $rules);
        }

        // Use column names by default for labels
        $columns = array_keys($this->_table_columns);

        // Merge user-defined labels
        foreach (array_merge(array_combine($columns, $columns), $this->labels()) as $field => $label)
        {
            $this->_validation->label($field, $label);
        }
    }

    /**
     * Reload column definitions.
     *
     * @chainable
     *
     * @param boolean $force Force reloading
     *
     * @return  self
     */
    public function reloadColumns(bool $force = false) : self
    {
        if ($force === true || empty($this->_table_columns))
        {
            if (isset(static::$_column_cache[$this->_object_name]))
            {
                // Use cached column information
                $this->_table_columns = static::$_column_cache[$this->_object_name];
            }
            else
            {
                // Grab column information from database
                $this->_table_columns = $this->listColumns();

                // Load column cache
                static::$_column_cache[$this->_object_name] = $this->_table_columns;
            }
        }

        return $this;
    }

    /**
     * Unloads the current object and clears the status.
     *
     * @chainable
     *
     * @return ORM
     */
    public function clear() : self
    {
        // Create an array with all the columns set to NULL
        $values = array_combine(array_keys($this->_table_columns), array_fill(0, count($this->_table_columns), null));

        // Replace the object and reset the object status
        $this->_object = $this->_changed = $this->_related = $this->_original_values = [];

        // Replace the current object with an empty one
        $this->_loadValues($values);

        // Reset primary key
        $this->_primary_key_value = null;

        // Reset the loaded state
        $this->_loaded = false;

        $this->reset();

        return $this;
    }

    /**
     * Reloads the current object from the database.
     *
     * @chainable
     *
     * @return self
     *
     * @throws Exception
     */
    public function reload() : self
    {
        $primary_key = $this->pk();

        // Replace the object and reset the object status
        $this->_object = $this->_changed = $this->_related = $this->_original_values = [];

        // Only reload the object if we have one to reload
        if ($this->_loaded) {
            return $this->clear()
                        ->where($this->_object_name . '.' . $this->_primary_key, '=', $primary_key)
                        ->find();
        }

        return $this->clear();
    }

    /**
     * Checks if object data is set.
     *
     * @param string $column Column name
     *
     * @return boolean
     */
    public function __isset(string $column) : bool
    {
        return (isset($this->_object[$column]) OR
                isset($this->_related[$column]) OR
                isset($this->_has_one[$column]) OR
                isset($this->_belongs_to[$column]) OR
                isset($this->_has_many[$column]));
    }

    /**
     * Unsets object data.
     *
     * @param string $column Column name
     *
     * @return void
     */
    public function __unset(string $column) : void
    {
        unset($this->_object[$column], $this->_changed[$column], $this->_related[$column]);
    }

    /**
     * Displays the primary key of a model when it is converted to a string.
     *
     * @return string
     */
    public function __toString() : string
    {
        return (string)$this->pk();
    }

    /**
     * Allows serialization of only the object data and state, to prevent
     * "stale" objects being unserialized, which also requires less memory.
     *
     * @return string
     */
    public function serialize() : string
    {
        // Init vars
        $data = [];

        // Store only information about the object
        foreach (['_primary_key_value', '_object', '_changed', '_loaded', '_saved', '_sorting', '_original_values'] as
                 $var
        ) {
            $data[$var] = $this->{$var};
        }

        return serialize($data);
    }

    /**
     * Check whether the model data has been modified.
     * If $field is specified, checks whether that field was modified.
     *
     * @param string $field Field to check for changes
     *
     * @return array|string|null  Null if $field has not changed. String value if $field has changed.
     *                            Array of all changed fields if $field is not provided.
     */
    public function changed(?string $field = null)
    {
        return ($field === null)
            ? $this->_changed
            : Arr::get($this->_changed, $field);
    }

    /**
     * Check whether a specific field has changed. This function is similar to changed() except a field is required, and
     * it returns a boolean.
     *
     * @param string $field Field to check for changes.
     *
     * @return bool          True if field has changed, false if it has not.
     */
    public function hasChanged(string $field) : bool
    {
        return array_key_exists($field, $this->_changed);
    }

    /**
     * Prepares the database connection and reloads the object.
     *
     * @param string $data String for unserialization
     *
     * @return  void
     *
     * @throws \Modseven\Exception
     * @throws \Modseven\Database\Exception
     */
    public function unserialize($data) : void
    {
        // Initialize model
        $this->_initialize();

        foreach (unserialize($data, null) as $name => $var)
        {
            $this->{$name} = $var;
        }

        if ($this->_reload_on_wakeup === true)
        {
            // Reload the object
            $this->reload();
        }
    }

    /**
     * Handles retrieval of all model values, relationships, and metadata.
     * [!!] This should not be overridden.
     *
     * @param string $column Column name
     *
     * @return  mixed
     *
     * @throws Exception
     */
    public function __get(string $column)
    {
        return $this->get($column);
    }

    /**
     * Handles getting of column
     * Override this method to add custom get behavior
     *
     * @param string $column Column name
     *
     * @return mixed
     *
     * @throws Exception
     */
    public function get(string $column)
    {
        if (array_key_exists($column, $this->_object))
        {
            return (in_array($column, $this->_serialize_columns, true))
                ? $this->_unserializeValue($this->_object[$column])
                : $this->_object[$column];
        }
        if (isset($this->_related[$column]))
        {
            // Return related model that has already been fetched
            return $this->_related[$column];
        }
        if (isset($this->_belongs_to[$column]))
        {
            $model = $this->_related($column);

            // Use this model's column and foreign model's primary key
            $col = $model->_object_name . '.' . $model->_primary_key;
            $val = $this->_object[$this->_belongs_to[$column]['foreign_key']];

            // Make sure we don't run WHERE "AUTO_INCREMENT column" = NULL queries. This would
            // return the last inserted record instead of an empty result.
            // See: http://mysql.localhost.net.ar/doc/refman/5.1/en/server-session-variables.html#sysvar_sql_auto_is_null
            if ($val !== null)
            {
                $model->where($col, '=', $val)->find();
            }

            return $this->_related[$column] = $model;
        }
        if (isset($this->_has_one[$column]))
        {
            $model = $this->_related($column);

            // Use this model's primary key value and foreign model's column
            $col = $model->_object_name . '.' . $this->_has_one[$column]['foreign_key'];
            $val = $this->pk();

            $model->where($col, '=', $val)->find();

            return $this->_related[$column] = $model;
        }
        if (isset($this->_has_many[$column]))
        {
            $model = self::factory($this->_has_many[$column]['model']);

            if (isset($this->_has_many[$column]['through'])) {
                // Grab has_many "through" relationship table
                $through = $this->_has_many[$column]['through'];

                // Join on through model's target foreign key (far_key) and target model's primary key
                $join_col1 = $through . '.' . $this->_has_many[$column]['far_key'];
                $join_col2 = $model->_object_name . '.' . $model->_primary_key;

                $model->join($through)->on($join_col1, '=', $join_col2);

                // Through table's source foreign key (foreign_key) should be this model's primary key
                $col = $through . '.' . $this->_has_many[$column]['foreign_key'];
                $val = $this->pk();
            }
            else
            {
                // Simple has_many relationship, search where target model's foreign key is this model's primary key
                $col = $model->_object_name . '.' . $this->_has_many[$column]['foreign_key'];
                $val = $this->pk();
            }

            return $model->where($col, '=', $val);
        }

        throw new Exception('The :property property does not exist in the :class class',
            [':property' => $column, ':class' => get_class($this)]
        );
    }

    /**
     * Base set method.
     * [!!] This should not be overridden.
     *
     * @param string $column Column name
     * @param mixed  $value  Column value
     *
     * @throws Exception
     */
    public function __set(string $column, $value) : void
    {
        $this->set($column, $value);
    }

    /**
     * Handles setting of columns
     * Override this method to add custom set behavior
     *
     * @param string $column Column name
     * @param mixed  $value  Column value
     *
     * @return ORM
     *
     * @throws Exception
     */
    public function set(string $column, $value) : self
    {
        if ( ! isset($this->_object_name))
        {
            // Object not yet constructed, so we're loading data from a database call cast
            $this->_cast_data[$column] = $value;

            return $this;
        }

        if (in_array($column, $this->_serialize_columns, true))
        {
            $value = $this->_serializeValue($value);
        }

        if (array_key_exists($column, $this->_object))
        {
            // Filter the data
            $value = $this->runFilter($column, $value);

            // See if the data really changed
            if ($value !== $this->_object[$column])
            {
                $this->_object[$column] = $value;

                // Data has changed
                $this->_changed[$column] = $column;

                // Object is no longer saved or valid
                $this->_saved = $this->_valid = false;
            }
        }
        elseif (isset($this->_belongs_to[$column]))
        {
            // Update related object itself
            $this->_related[$column] = $value;

            // Update the foreign key of this model
            $this->_object[$this->_belongs_to[$column]['foreign_key']] = ($value instanceof self)
                ? $value->pk()
                : null;

            $this->_changed[$column] = $this->_belongs_to[$column]['foreign_key'];
        }
        elseif (isset($this->_has_many[$column]))
        {
            if (Arr::get($this->_has_many[$column], 'update', false))
            {
                $model = $this->_has_many[$column]['model'];
                $pk = self::factory($model)->primaryKey();

                $current_ids = $this->get($column)->findAll()->asArray(null, 'id');

                $new_ids = array_diff($value, $current_ids);
                if (count($new_ids) > 0)
                {
                    $objects = self::factory($model)->where($pk, 'IN', $new_ids)->findAll();
                    foreach ($objects as $object)
                    {
                        $this->add($column, $object);
                    }
                }

                $delete_ids = array_diff($current_ids, $value);
                if (count($delete_ids) > 0)
                {
                    $objects = self::factory($model)->where($pk, 'IN', $delete_ids)->findAll();
                    foreach ($objects as $object) {
                        $this->remove($column, $object);
                    }
                }
            }
            else
            {
                throw new Exception('The :property: property is a to many relation in the :class: class',
                    [':property:' => $column, ':class:' => get_class($this)]
                );
            }
        }
        else
        {
            throw new Exception('The :property: property does not exist in the :class: class',
                [':property:' => $column, ':class:' => get_class($this)]
            );
        }

        return $this;
    }

    /**
     * Set values from an array with support for one-one relationships. This method should be used
     * for loading in post data, etc.
     *
     * @param array $values   Array of column => value
     * @param array $expected Array of keys to take from $values
     *
     * @return self
     */
    public function values(array $values, ?array $expected = null) : self
    {
        // Default to expecting all columns
        if ($expected === null)
        {
            $expected = array_keys($this->_table_columns);
        }

        // Don't set the primary key if model loaded and column not set
        if ($this->_primary_key && $this->_loaded)
        {
            unset($values[$this->_primary_key]);
        }

        foreach ($expected as $key => $column)
        {
            if (is_string($key))
            {
                // isset() fails when the value is NULL (we want it to pass)
                if ( ! array_key_exists($key, $values))
                {
                    continue;
                }

                // Try to set values to a related model
                $this->{$key}->values($values[$key], $column);
            }
            else
            {
                // isset() fails when the value is NULL (we want it to pass)
                if ( ! array_key_exists($column, $values))
                {
                    continue;
                }

                // Update the column, respects __set()
                $this->$column = $values[$column];
            }
        }

        return $this;
    }

    /**
     * Returns the type of the column
     *
     * @param string $column
     *
     * @return string|bool
     */
    public function tableColumnType(string $column)
    {
        if ( ! array_key_exists($column, $this->_table_columns))
        {
            return false;
        }

        return $this->_table_columns[$column]['type'];
    }

    /**
     * Returns a value as the native type, will return FALSE if the
     * value could not be casted.
     *
     * @param string $column
     *
     * @return mixed
     *
     * @throws Exception
     */
    protected function getTyped(string $column)
    {
        $value = $this->get($column);

        if ($value === null)
        {
            return null;
        }

        // Call __get for any user processing
        switch ($this->tableColumnType($column))
        {
            case 'float':
                return (float)$this->__get($column);
            case 'int':
                return (int)$this->__get($column);
            case 'string':
                return (string)$this->__get($column);
        }

        return $value;
    }

    /**
     * Returns the values of this object as an array, including any related one-one
     * models that have already been loaded using with()
     *
     * @param bool $show_all Show all models
     *
     * @return array
     *
     * @throws Exception
     */
    public function asArray(bool $show_all = false) : array
    {
        $object = [];

        if ($show_all || empty($this->_private_columns))
        {
            foreach ($this->_object as $column => $value)
            {
                // Call __get for any user processing
                $object[$column] = $this->__get($column);
            }
        }
        else
        {
            foreach ($this->_object as $column => $value)
            {
                // Call __get for any user processing
                if ( ! in_array($column, $this->_private_columns, true))
                {
                    $object[$column] = $this->__get($column);
                }
            }
        }

        foreach ($this->_related as $column => $model)
        {
            // Include any related objects that are already loaded
            $object[$column] = $model->asArray();
        }

        return $object;
    }

    /**
     * Returns the values of this object as an new object, including any related
     * one-one models that have already been loaded using with(). Removes private
     * columns.
     *
     * @param bool $show_all Show all Models
     *
     * @return stdClass
     *
     * @throws Exception
     */
    public function asObject(bool $show_all = false) : stdClass
    {
        $object = new stdClass;

        if ($show_all || empty($this->_private_columns))
        {
            foreach ($this->_object as $column => $value)
            {
                $object->{$column} = $this->getTyped($column);
            }
        }
        else
        {
            foreach ($this->_object as $column => $value)
            {
                if ( ! in_array($column, $this->_private_columns, true)) {
                    $object->{$column} = $this->getTyped($column);
                }
            }
        }

        foreach ($this->_related as $column => $model)
        {
            // Include any related objects that are already loaded
            $object->{$column} = $model->asObject();
        }

        return $object;
    }

    /**
     * Binds another one-to-one object to this model.  One-to-one objects
     * can be nested using 'object1:object2' syntax
     *
     * @param string $target_path Target model to bind to
     *
     * @return self
     */
    public function with(string $target_path) : self
    {
        if (isset($this->_with_applied[$target_path]))
        {
            // Don't join anything already joined
            return $this;
        }

        // Split object parts
        $aliases = explode(':', $target_path);
        $target = $this;
        foreach ($aliases as $alias)
        {
            // Go down the line of objects to find the given target
            $parent = $target;
            $target = $parent->_related($alias);

            if ( ! $target)
            {
                // Can't find related object
                return $this;
            }
        }

        // Target alias is at the end
        $target_alias = $alias;

        // Pop-off top alias to get the parent path (user:photo:tag becomes user:photo - the parent table prefix)
        array_pop($aliases);
        $parent_path = implode(':', $aliases);

        if (empty($parent_path))
        {
            // Use this table name itself for the parent path
            $parent_path = $this->_object_name;
        }
        elseif ( ! isset($this->_with_applied[$parent_path]))
        {
            // If the parent path hasn't been joined yet, do it first (otherwise LEFT JOINs fail)
            $this->with($parent_path);
        }

        // Add to with_applied to prevent duplicate joins
        $this->_with_applied[$target_path] = true;

        // Use the keys of the empty object to determine the columns
        foreach (array_keys($target->_object) as $column)
        {
            $name = $target_path . '.' . $column;
            $alias = $target_path . ':' . $column;

            // Add the prefix so that load_result can determine the relationship
            $this->select([$name, $alias]);
        }

        if (isset($parent->_belongs_to[$target_alias]))
        {
            // Parent belongs_to target, use target's primary key and parent's foreign key
            $join_col1 = $target_path . '.' . $target->_primary_key;
            $join_col2 = $parent_path . '.' . $parent->_belongs_to[$target_alias]['foreign_key'];
        }
        else
        {
            // Parent has_one target, use parent's primary key as target's foreign key
            $join_col1 = $parent_path . '.' . $parent->_primary_key;
            $join_col2 = $target_path . '.' . $parent->_has_one[$target_alias]['foreign_key'];
        }

        // Join the related object into the result
        $this->join([$target->_table_name, $target_path], 'LEFT')->on($join_col1, '=', $join_col2);

        return $this;
    }

    /**
     * Initializes the Database Builder to given query type
     *
     * @param int $type Type of Database query
     *
     * @return self
     */
    protected function _build(int $type) : self
    {
        // Construct new builder object based on query type
        switch ($type)
        {
            case Database::SELECT:
                $this->_db_builder = DB::select();
                break;
            case Database::UPDATE:
                $this->_db_builder = DB::update([$this->_table_name, $this->_object_name]);
                break;
            case Database::DELETE:
                // Cannot use an alias for DELETE queries
                $this->_db_builder = DB::delete($this->_table_name);
        }

        // Process pending database method calls
        foreach ($this->_db_pending as $method)
        {
            $name = $method['name'];
            $args = $method['args'];

            $this->_db_applied[$name] = $name;

            call_user_func_array([$this->_db_builder, $name], $args);
        }

        return $this;
    }

    /**
     * Finds and loads a single database row into the object.
     *
     * @chainable
     *
     * @return self|Result
     *
     * @throws Exception
     */
    public function find()
    {
        if ($this->_loaded)
        {
            throw new Exception('Method find() cannot be called on loaded objects');
        }

        if ( ! empty($this->_load_with))
        {
            foreach ($this->_load_with as $alias)
            {
                // Bind auto relationships
                $this->with($alias);
            }
        }

        $this->_build(Database::SELECT);

        return $this->_loadResult(false);
    }

    /**
     * Finds multiple database rows and returns an iterator of the rows found.
     *
     * @return self|Result
     *
     * @throws Exception
     */
    public function findAll()
    {
        if ($this->_loaded)
        {
            throw new Exception('Method find_all() cannot be called on loaded objects');
        }

        if ( ! empty($this->_load_with))
        {
            foreach ($this->_load_with as $alias)
            {
                // Bind auto relationships
                $this->with($alias);
            }
        }

        $this->_build(Database::SELECT);

        return $this->_loadResult(true);
    }

    /**
     * Returns an array of columns to include in the select query. This method
     * can be overridden to change the default select behavior.
     *
     * @return array Columns to select
     */
    protected function _buildSelect() : array
    {
        $columns = [];

        foreach ($this->_table_columns as $column => $_)
        {
            $columns[] = [$this->_object_name . '.' . $column, $column];
        }

        return $columns;
    }

    /**
     * Loads a database result, either as a new record for this model, or as
     * an iterator for multiple rows.
     *
     * @chainable
     *
     * @param bool $multiple Return an iterator or load a single row
     *
     * @return self|Result|object
     *
     * @throws Exception
     */
    protected function _loadResult(bool $multiple = false)
    {
        $this->_db_builder->from([$this->_table_name, $this->_object_name]);

        if ($multiple === false)
        {
            // Only fetch 1 record
            $this->_db_builder->limit(1);
        }

        // Select all columns by default
        $this->_db_builder->selectArray($this->_buildSelect());

        if ( ! isset($this->_db_applied['order_by']) && ! empty($this->_sorting))
        {
            foreach ($this->_sorting as $column => $direction)
            {
                if (strpos($column, '.') === false)
                {
                    // Sorting column for use in JOINs
                    $column = $this->_object_name . '.' . $column;
                }

                $this->_db_builder->orderBy($column, $direction);
            }
        }

        if ($multiple === true)
        {
            try
            {
                // Return database iterator casting to this object type
                $result = $this->_db_builder->asObject(get_class($this))->execute($this->_db);
            }
            catch (\Modseven\Exception $e)
            {
                throw new Exception($e->getMessage(), NULL, $e->getCode(), $e);
            }

            $this->reset();

            return $result;
        }

        try
        {
            // Load the result as an associative array
            $result = $this->_db_builder->asAssoc()->execute($this->_db);
        }
        catch (\Modseven\Exception $e)
        {
            throw new Exception($e->getMessage(), NULL, $e->getCode(), $e);
        }

        $this->reset();

        if ($result->count() === 1)
        {
            // Load object values
            $this->_loadValues($result->current());
        }
        else
        {
            // Clear the object, nothing was found
            $this->clear();
        }

        return $this;
    }

    /**
     * Loads an array of values into into the current object.
     *
     * @chainable
     *
     * @param array $values Values to load
     *
     * @return self
     */
    protected function _loadValues(array $values) : self
    {
        if (array_key_exists($this->_primary_key, $values))
        {
            if ($values[$this->_primary_key] !== null)
            {
                // Flag as loaded and valid
                $this->_loaded = $this->_valid = true;

                // Store primary key
                $this->_primary_key_value = $values[$this->_primary_key];
            }
            else
            {
                // Not loaded or valid
                $this->_loaded = $this->_valid = false;
            }
        }

        // Related objects
        $related = [];

        foreach ($values as $column => $value)
        {
            if (strpos($column, ':') === false)
            {
                // Load the value to this model
                $this->_object[$column] = $value;
            }
            else
            {
                // Column belongs to a related model
                [$prefix, $column] = explode(':', $column, 2);

                $related[$prefix][$column] = $value;
            }
        }

        if ( ! empty($related))
        {
            foreach ($related as $object => $vals)
            {
                // Load the related objects with the values in the result
                $this->_related($object)->_loadValues($vals);
            }
        }

        if ($this->_loaded)
        {
            // Store the object in its original state
            $this->_original_values = $this->_object;
        }

        return $this;
    }

    /**
     * Behavior definitions
     *
     * @return array
     */
    public function behaviors() : array
    {
        return [];
    }

    /**
     * Rule definitions for validation
     *
     * @return array
     */
    public function rules() : array
    {
        return [];
    }

    /**
     * Filters a value for a specific column
     *
     * @param string $field The column name
     * @param string $value The value to filter
     *
     * @return string
     *
     * @throws Exception
     */
    protected function runFilter(string $field, string $value) : string
    {
        $filters = $this->filters();

        // Get the filters for this column
        $wildcards = empty($filters[true]) ? [] : $filters[true];

        // Merge in the wildcards
        $filters = empty($filters[$field]) ? $wildcards : array_merge($wildcards, $filters[$field]);

        // Bind the field name and model so they can be used in the filter method
        $_bound = [
            ':field' => $field,
            ':model' => $this,
        ];

        foreach ($filters as $array)
        {
            // Value needs to be bound inside the loop so we are always using the
            // version that was modified by the filters that already ran
            $_bound[':value'] = $value;

            // Filters are defined as array($filter, $params)
            $filter = $array[0];
            $params = Arr::get($array, 1, [':value']);

            foreach ($params as $key => $param)
            {
                if (is_string($param) && array_key_exists($param, $_bound)) {
                    // Replace with bound value
                    $params[$key] = $_bound[$param];
                }
            }

            if (is_array($filter) || ! is_string($filter))
            {
                // This is either a callback as an array or a lambda
                $value = call_user_func_array($filter, $params);
            }
            elseif (strpos($filter, '::') === false)
            {
                // Use a function call
                try
                {
                    $function = new ReflectionFunction($filter);
                }
                catch (ReflectionException $e)
                {
                    throw new Exception($e->getMessage(), NULL, $e->getCode(), $e);
                }

                // Call $function($this[$field], $param, ...) with Reflection
                $value = $function->invokeArgs($params);
            }
            else
            {
                // Split the class and method of the rule
                [$class, $method] = explode('::', $filter, 2);

                // Use a static method call
                try
                {
                    $method = new ReflectionMethod($class, $method);
                }
                catch (ReflectionException $e)
                {
                    throw new Exception($e->getMessage(), NULL, $e->getCode(), $e);
                }

                // Call $Class::$method($this[$field], $param, ...) with Reflection
                $value = $method->invokeArgs(null, $params);
            }
        }

        return $value;
    }

    /**
     * Filter definitions for validation
     *
     * @return array
     */
    public function filters() : array
    {
        return [];
    }

    /**
     * Label definitions for validation
     *
     * @return array
     */
    public function labels() : array
    {
        return [];
    }

    /**
     * Validates the current model's data
     *
     * @param Validation $extra_validation Validation object
     *
     * @return self
     *
     * @throws \Modseven\Exception
     * @throws \Modseven\ORM\Validation\Exception
     */
    public function check(Validation $extra_validation = null) : self
    {
        // Determine if any external validation failed
        $extra_errors = ($extra_validation AND ! $extra_validation->check());

        // Always build a new validation object
        $this->_validation();

        $array = $this->_validation;

        if ($extra_errors || ($this->_valid = $array->check()) === false)
        {
            $exception = new \Modseven\ORM\Validation\Exception($this->errorsFilename(), $array);

            if ($extra_errors)
            {
                // Merge any possible errors from the external object
                $exception->addObject('_external', $extra_validation);
            }
            throw $exception;
        }

        return $this;
    }

    /**
     * Insert a new object to the database
     *
     * @param Validation $validation Validation object
     *
     * @return self
     *
     * @throws Exception
     * @throws \Modseven\Exception
     * @throws \Modseven\ORM\Validation\Exception
     */
    public function create(?Validation $validation = null) : self
    {
        if ($this->_loaded)
        {
            throw new Exception('Cannot create :model model because it is already loaded.',
                [':model' => $this->_object_name]
            );
        }

        // Invoke all behaviors
        foreach ($this->_behaviors as $behavior)
        {
            $behavior->onCreate($this);
        }

        // Require model validation before saving
        if ( ! $this->_valid || $validation)
        {
            $this->check($validation);
        }

        $data = [];
        foreach ($this->_changed as $column)
        {
            // Generate list of column => values
            $data[$column] = $this->_object[$column];
        }

        if (is_array($this->_created_column))
        {
            // Fill the created column
            $column = $this->_created_column['column'];
            $format = $this->_created_column['format'];

            $data[$column] = $this->_object[$column] = ($format === true) ? time() : date($format);
        }

        try
        {
            $result = DB::insert($this->_table_name)
                        ->columns(array_keys($data))
                        ->values(array_values($data))
                        ->execute($this->_db);
        }
        catch (\Modseven\Exception|\Modseven\Database\Exception $e)
        {
            throw new Exception($e->getMessage(), NULL, $e->getCode(), $e);
        }

        if ( ! array_key_exists($this->_primary_key, $data) || ($this->_object[$this->_primary_key] === null))
        {
            // Load the insert id as the primary key if it was left out
            $this->_object[$this->_primary_key] = $this->_primary_key_value = $result[0];
        }
        else
        {
            $this->_primary_key_value = $this->_object[$this->_primary_key];
        }

        // Object is now loaded and saved
        $this->_loaded = $this->_saved = true;

        // All changes have been saved
        $this->_changed = [];
        $this->_original_values = $this->_object;

        return $this;
    }

    /**
     * Updates a single record or multiple records
     *
     * @chainable
     *
     * @param Validation $validation Validation object
     *
     * @return self
     *
     * @throws Exception
     * @throws \Modseven\Exception
     * @throws \Modseven\ORM\Validation\Exception
     */
    public function update(Validation $validation = null) : self
    {
        if ( ! $this->_loaded)
        {
            throw new Exception('Cannot update :model model because it is not loaded.',
                [':model' => $this->_object_name]
            );
        }

        // Invoke all behaviors
        foreach ($this->_behaviors as $behavior)
        {
            $behavior->onUpdate($this);
        }

        // Run validation if the model isn't valid or we have additional validation rules.
        if ( ! $this->_valid || $validation)
        {
            $this->check($validation);
        }

        if (empty($this->_changed))
        {
            // Nothing to update
            return $this;
        }

        $data = [];
        foreach ($this->_changed as $column)
        {
            // Compile changed data
            $data[$column] = $this->_object[$column];
        }

        if (is_array($this->_updated_column))
        {
            // Fill the updated column
            $column = $this->_updated_column['column'];
            $format = $this->_updated_column['format'];

            $data[$column] = $this->_object[$column] = ($format === true) ? time() : date($format);
        }

        // Use primary key value
        $id = $this->pk();

        // Update a single record
        try {
            DB::update($this->_table_name)
              ->set($data)
              ->where($this->_primary_key, '=', $id)
              ->execute($this->_db);
        }
        catch (\Modseven\Exception $e)
        {
            throw new Exception($e->getMessage(), null, $e->getCode(), $e);
        }

        if (isset($data[$this->_primary_key])) {
            // Primary key was changed, reflect it
            $this->_primary_key_value = $data[$this->_primary_key];
        }

        // Object has been saved
        $this->_saved = true;

        // All changes have been saved
        $this->_changed = [];
        $this->_original_values = $this->_object;

        return $this;
    }

    /**
     * Updates or Creates the record depending on loaded()
     *
     * @chainable
     *
     * @param Validation $validation Validation object
     *
     * @return self
     *
     * @throws Exception
     * @throws \Modseven\Exception
     * @throws \Modseven\ORM\Validation\Exception
     */
    public function save(?Validation $validation = null) : self
    {
        return $this->loaded() ? $this->update($validation) : $this->create($validation);
    }

    /**
     * Deletes a single record while ignoring relationships.
     *
     * @chainable
     *
     * @return self
     *
     * @throws Exception
     */
    public function delete() : self
    {
        if ( ! $this->_loaded)
        {
            throw new Exception('Cannot delete :model model because it is not loaded.',
                [':model' => $this->_object_name]
            );
        }

        // Use primary key value
        $id = $this->pk();

        // Delete the object
        try {
            DB::delete($this->_table_name)
              ->where($this->_primary_key, '=', $id)
              ->execute($this->_db);
        }
        catch (\Modseven\Exception $e)
        {
            throw new Exception($e->getMessage(), NULL, $e->getCode(), $e);
        }

        return $this->clear();
    }

    /**
     * Tests if this object has a relationship to a different model,
     * or an array of different models. When providing far keys, the number
     * of relations must equal the number of keys.
     *
     * @param string $alias    Alias of the has_many "through" relationship
     * @param mixed  $far_keys Related model, primary key, or an array of primary keys
     *
     * @return bool
     *
     * @throws Exception
     */
    public function has(string $alias, $far_keys = null) : bool
    {
        $count = $this->countRelations($alias, $far_keys);

        if ($far_keys === null)
        {
            return (bool)$count;
        }

        if (is_array($far_keys) OR $far_keys instanceof Countable) {
            $keys = count($far_keys);
        }
        else
        {
            $keys = 1;
        }

        return $keys === $count;
    }

    /**
     * Tests if this object has a relationship to a different model,
     * or an array of different models. When providing far keys, this function
     * only checks that at least one of the relationships is satisfied.
     *
     * @param string $alias    Alias of the has_many "through" relationship
     * @param mixed  $far_keys Related model, primary key, or an array of primary keys
     *
     * @return bool
     *
     * @throws Exception
     */
    public function hasAny(string $alias, $far_keys = null) : bool
    {
        return (bool)$this->countRelations($alias, $far_keys);
    }

    /**
     * Returns the number of relationships
     *
     * @param string $alias    Alias of the has_many "through" relationship
     * @param mixed  $far_keys Related model, primary key, or an array of primary keys
     *
     * @return integer
     *
     * @throws Exception
     */
    public function countRelations(string $alias, $far_keys = null) : int
    {
        if ($far_keys === null)
        {
            try{
                return (int)DB::select([DB::expr('COUNT(*)'), 'records_found'])
                              ->from($this->_has_many[$alias]['through'])
                              ->where($this->_has_many[$alias]['foreign_key'], '=', $this->pk())
                              ->execute($this->_db)->get('records_found');
            }
            catch (\Modseven\Exception $e)
            {
                throw new Exception($e->getMessage(), null, $e->getCode(), $e);
            }
        }

        $far_keys = ($far_keys instanceof self) ? $far_keys->pk() : $far_keys;

        // We need an array to simplify the logic
        $far_keys = (array)$far_keys;

        // Nothing to check if the model isn't loaded or we don't have any far_keys
        if ( ! $far_keys || ! $this->_loaded) {
            return 0;
        }

        try
        {
            $count = (int)DB::select([DB::expr('COUNT(*)'), 'records_found'])
                            ->from($this->_has_many[$alias]['through'])
                            ->where($this->_has_many[$alias]['foreign_key'], '=', $this->pk())
                            ->where($this->_has_many[$alias]['far_key'], 'IN', $far_keys)
                            ->execute($this->_db)->get('records_found');
        }
        catch (\Modseven\Exception $e)
        {
            throw new Exception($e->getMessage(), null, $e->getCode(), $e);
        }

        // Rows found need to match the rows searched
        return $count;
    }

    /**
     * Adds a new relationship to between this model and another.
     *
     * @param string $alias    Alias of the has_many "through" relationship
     * @param mixed  $far_keys Related model, primary key, or an array of primary keys
     *
     * @return self
     *
     * @throws Exception
     */
    public function add(string $alias, $far_keys) : self
    {
        $far_keys = ($far_keys instanceof self) ? $far_keys->pk() : $far_keys;

        $columns = [$this->_has_many[$alias]['foreign_key'], $this->_has_many[$alias]['far_key']];
        $foreign_key = $this->pk();

        $query = DB::insert($this->_has_many[$alias]['through'], $columns);

        foreach ((array)$far_keys as $key)
        {
            try {
                $query->values([$foreign_key, $key]);
            }
            catch (\Modseven\Database\Exception $e)
            {
                throw new Exception($e->getMessage(), null, $e->getCode(), $e);
            }

        }

        try
        {
            $query->execute($this->_db);
        }
        catch (\Modseven\Exception $e)
        {
            throw new Exception($e->getMessage(), null, $e->getCode(), $e);
        }

        return $this;
    }

    /**
     * Removes a relationship between this model and another.
     *
     * @param string $alias    Alias of the has_many "through" relationship
     * @param mixed  $far_keys Related model, primary key, or an array of primary keys
     *
     * @return self
     *
     * @throws Exception
     */
    public function remove(string $alias, $far_keys = null) : self
    {
        $far_keys = ($far_keys instanceof self) ? $far_keys->pk() : $far_keys;

        $query = DB::delete($this->_has_many[$alias]['through'])
                   ->where($this->_has_many[$alias]['foreign_key'], '=', $this->pk());

        if ($far_keys !== null) {
            // Remove all the relationships in the array
            $query->where($this->_has_many[$alias]['far_key'], 'IN', (array)$far_keys);
        }

        try
        {
            $query->execute($this->_db);
        }
        catch (\Modseven\Exception $e)
        {
            throw new Exception($e->getMessage(), null, $e->getCode(), $e);
        }

        return $this;
    }

    /**
     * Count the number of records in the table.
     *
     * @return int
     *            
     * @throws \Modseven\Exception
     */
    public function countAll() : int
    {
        $selects = [];

        foreach ($this->_db_pending as $key => $method)
        {
            if ($method['name'] === 'select')
            {
                // Ignore any selected columns for now
                $selects[$key] = $method;
                unset($this->_db_pending[$key]);
            }
        }

        if ( ! empty($this->_load_with))
        {
            foreach ($this->_load_with as $alias)
            {
                // Bind relationship
                $this->with($alias);
            }
        }

        $this->_build(Database::SELECT);

        $records = $this->_db_builder->from([$this->_table_name, $this->_object_name])
                                     ->select([
                                             DB::expr('COUNT(' . $this->_db->quoteColumn($this->_object_name . '.' .
                                                                                          $this->_primary_key
                                                 ) . ')'
                                             ), 'records_found'
                                         ]
                                     )
                                     ->execute($this->_db)
                                     ->get('records_found');

        // Add back in selected columns
        $this->_db_pending += $selects;

        $this->reset();

        // Return the total number of records in a table
        return (int)$records;
    }

    /**
     * Proxy method to Database list_columns.
     *
     * @return array
     */
    public function listColumns() : array
    {
        // Proxy to database
        return $this->_db->listColumns($this->_table_name);
    }

    /**
     * Returns an ORM model for the given one-one related alias
     *
     * @param string $alias Alias name
     *
     * @return self|bool|Model
     */
    protected function _related(string $alias)
    {
        if (isset($this->_related[$alias]))
        {
            return $this->_related[$alias];
        }
        if (isset($this->_has_one[$alias]))
        {
            return $this->_related[$alias] = self::factory($this->_has_one[$alias]['model']);
        }
        if (isset($this->_belongs_to[$alias]))
        {
            return $this->_related[$alias] = self::factory($this->_belongs_to[$alias]['model']);
        }

        return false;
    }

    /**
     * Returns the value of the primary key
     *
     * @return mixed Primary key
     */
    public function pk()
    {
        return $this->_primary_key_value;
    }

    /**
     * Returns last executed query
     *
     * @return string
     */
    public function lastQuery() : string
    {
        return $this->_db->last_query;
    }

    /**
     * Clears query builder.  Passing FALSE is useful to keep the existing
     * query conditions for another query.
     *
     * @param bool $next Pass FALSE to avoid resetting on the next call
     *
     * @return self
     */
    public function reset($next = true) : self
    {
        if ($next && $this->_db_reset) {
            $this->_db_pending = [];
            $this->_db_applied = [];
            $this->_db_builder = null;
            $this->_with_applied = [];
        }

        // Reset on the next call?
        $this->_db_reset = $next;

        return $this;
    }

    /**
     * Serialize a value
     *
     * @param mixed $value
     *
     * @return string
     */
    protected function _serializeValue($value) : string
    {
        return json_encode($value);
    }

    /**
     * Unserialize a value
     *
     * @param string $value
     *
     * @return array
     *
     * @throws Exception
     */
    protected function _unserializeValue(string $value) : array
    {
        try
        {
            $json = json_decode($value, true, 512, JSON_THROW_ON_ERROR);
        }
        catch (JsonException $e)
        {
            throw new Exception($e->getMessage(), null, $e->getCode(), $e);
        }
        return $json;
    }

    /**
     * Get Object Name
     *
     * @return string
     */
    public function objectName() : string
    {
        return $this->_object_name;
    }

    /**
     * Get Object name in plural
     *
     * @return string
     */
    public function objectPlural() : string
    {
        return $this->_object_plural;
    }

    /**
     * Is loaded?
     *
     * @return bool
     */
    public function loaded() : bool
    {
        return $this->_loaded;
    }

    /**
     * Is saved?
     * @return bool
     */
    public function saved() : bool
    {
        return $this->_saved;
    }

    /**
     * Primary Key getter
     *
     * @return string
     */
    public function primaryKey() : string
    {
        return $this->_primary_key;
    }

    /**
     * Table name getter
     *
     * @return string
     */
    public function tableName() : string
    {
        return $this->_table_name;
    }

    /**
     * Column getter
     *
     * @return array
     */
    public function tableColumns() : array
    {
        return $this->_table_columns;
    }

    /**
     * Has?
     *
     * @return array
     */
    public function hasOne() : array
    {
        return $this->_has_one;
    }

    /**
     * Belongs to?
     *
     * @return array
     */
    public function belongsTo() : array
    {
        return $this->_belongs_to;
    }

    /**
     * Has many?
     *
     * @return array
     */
    public function hasMany() : array
    {
        return $this->_has_many;
    }

    /**
     * Load it with?
     *
     * @return array
     */
    public function loadWith() : array
    {
        return $this->_load_with;
    }

    /**
     * Get original values
     *
     * @return array
     */
    public function originalValues() : array
    {
        return $this->_original_values;
    }

    /**
     * Get created column name
     *
     * @return string
     */
    public function createdColumn() : string
    {
        return $this->_created_column;
    }

    /**
     * Get updated column name
     *
     * @return string
     */
    public function updatedColumn() : string
    {
        return $this->_updated_column;
    }

    /**
     * Validation object getter
     * @return Validation
     */
    public function validation() : ?Validation
    {
        if ( ! isset($this->_validation))
        {
            // Initialize the validation object
            $this->_validation();
        }

        return $this->_validation;
    }

    /**
     * Get current object
     *
     * @return mixed
     */
    public function object()
    {
        return $this->_object;
    }

    /**
     * Get filename for errors
     *
     * @return string
     */
    public function errorsFilename() : string
    {
        return $this->_errors_filename;
    }

    /**
     * Alias of and_where()
     *
     * @param mixed  $column column name or array($column, $alias) or object
     * @param string $op     logic operator
     * @param mixed  $value  column value
     *
     * @return  self
     */
    public function where($column, string $op, $value) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'where',
            'args' => [$column, $op, $value],
        ];

        return $this;
    }

    /**
     * Creates a new "AND WHERE" condition for the query.
     *
     * @param mixed  $column column name or array($column, $alias) or object
     * @param string $op     logic operator
     * @param mixed  $value  column value
     *
     * @return  self
     */
    public function andWhere($column, string $op, $value) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'andWhere',
            'args' => [$column, $op, $value],
        ];

        return $this;
    }

    /**
     * Creates a new "OR WHERE" condition for the query.
     *
     * @param mixed  $column column name or array($column, $alias) or object
     * @param string $op     logic operator
     * @param mixed  $value  column value
     *
     * @return  self
     */
    public function orWhere($column, string $op, $value) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'orWhere',
            'args' => [$column, $op, $value],
        ];

        return $this;
    }

    /**
     * Alias of and_where_open()
     *
     * @return  self
     */
    public function whereOpen() : self
    {
        return $this->andWhereOpen();
    }

    /**
     * Opens a new "AND WHERE (...)" grouping.
     *
     * @return  self
     */
    public function andWhereOpen() : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'andWhereOpen',
            'args' => [],
        ];

        return $this;
    }

    /**
     * Opens a new "OR WHERE (...)" grouping.
     *
     * @return  self
     */
    public function orWhereOpen() : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'orWhereOpen',
            'args' => [],
        ];

        return $this;
    }

    /**
     * Closes an open "AND WHERE (...)" grouping.
     *
     * @return  self
     */
    public function whereClose() : self
    {
        return $this->andWhereClose();
    }

    /**
     * Closes an open "AND WHERE (...)" grouping.
     *
     * @return  self
     */
    public function andWhereClose() : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'andWhereClose',
            'args' => [],
        ];

        return $this;
    }

    /**
     * Closes an open "OR WHERE (...)" grouping.
     *
     * @return  self
     */
    public function orWhereClose() : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'orWhereClose',
            'args' => [],
        ];

        return $this;
    }

    /**
     * Applies sorting with "ORDER BY ..."
     *
     * @param mixed  $column    column name or array($column, $alias) or object
     * @param string $direction direction of sorting
     *
     * @return  self
     */
    public function orderBy($column, ?string $direction = null) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'orderBy',
            'args' => [$column, $direction],
        ];

        return $this;
    }

    /**
     * Return up to "LIMIT ..." results
     *
     * @param integer $number maximum results to return
     *
     * @return  self
     */
    public function limit(int $number) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'limit',
            'args' => [$number],
        ];

        return $this;
    }

    /**
     * Enables or disables selecting only unique columns using "SELECT DISTINCT"
     *
     * @param bool $value enable or disable distinct columns
     *
     * @return  self
     */
    public function distinct(bool $value) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'distinct',
            'args' => [$value],
        ];

        return $this;
    }

    /**
     * Choose the columns to select from.
     *
     * @param mixed $columns column name or array($column, $alias) or object
     * @param   ...
     *
     * @return  self
     */
    public function select(...$columns) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'select',
            'args' => $columns,
        ];

        return $this;
    }

    /**
     * Choose the tables to select "FROM ..."
     *
     * @param mixed $tables table name or array($table, $alias) or object
     * @param   ...
     *
     * @return  self
     */
    public function from(...$tables) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'from',
            'args' => $tables,
        ];

        return $this;
    }

    /**
     * Adds addition tables to "JOIN ...".
     *
     * @param mixed  $table column name or array($column, $alias) or object
     * @param string $type  join type (LEFT, RIGHT, INNER, etc)
     *
     * @return  self
     */
    public function join($table, ?string $type = null) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'join',
            'args' => [$table, $type],
        ];

        return $this;
    }

    /**
     * Adds "ON ..." conditions for the last created JOIN statement.
     *
     * @param mixed  $c1 column name or array($column, $alias) or object
     * @param string $op logic operator
     * @param mixed  $c2 column name or array($column, $alias) or object
     *
     * @return  self
     */
    public function on($c1, string $op, $c2) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'on',
            'args' => [$c1, $op, $c2],
        ];

        return $this;
    }

    /**
     * Creates a "GROUP BY ..." filter.
     *
     * @param mixed $columns column name or array($column, $alias) or object
     * @param   ...
     *
     * @return  self
     */
    public function groupBy(...$columns) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'groupBy',
            'args' => $columns,
        ];

        return $this;
    }

    /**
     * Alias of and_having()
     *
     * @param mixed  $column column name or array($column, $alias) or object
     * @param string $op     logic operator
     * @param mixed  $value  column value
     *
     * @return  self
     */
    public function having($column, string $op, $value = null) : self
    {
        return $this->andHaving($column, $op, $value);
    }

    /**
     * Creates a new "AND HAVING" condition for the query.
     *
     * @param mixed  $column column name or array($column, $alias) or object
     * @param string $op     logic operator
     * @param mixed  $value  column value
     *
     * @return  self
     */
    public function andHaving($column, string $op, $value = null) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'andHaving',
            'args' => [$column, $op, $value],
        ];

        return $this;
    }

    /**
     * Creates a new "OR HAVING" condition for the query.
     *
     * @param mixed  $column column name or array($column, $alias) or object
     * @param string $op     logic operator
     * @param mixed  $value  column value
     *
     * @return  self
     */
    public function orHaving($column, string $op, $value = null) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'orHaving',
            'args' => [$column, $op, $value],
        ];

        return $this;
    }

    /**
     * Alias of and_having_open()
     *
     * @return  self
     */
    public function havingOpen() : self
    {
        return $this->andHavingOpen();
    }

    /**
     * Opens a new "AND HAVING (...)" grouping.
     *
     * @return  self
     */
    public function andHavingOpen() : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'andHavingOpen',
            'args' => [],
        ];

        return $this;
    }

    /**
     * Opens a new "OR HAVING (...)" grouping.
     *
     * @return  self
     */
    public function orHavingOpen() : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'orHavingOpen',
            'args' => [],
        ];

        return $this;
    }

    /**
     * Closes an open "AND HAVING (...)" grouping.
     *
     * @return  self
     */
    public function havingClose() : self
    {
        return $this->andHavingClose();
    }

    /**
     * Closes an open "AND HAVING (...)" grouping.
     *
     * @return  self
     */
    public function andHavingClose() : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'andHavingClose',
            'args' => [],
        ];

        return $this;
    }

    /**
     * Closes an open "OR HAVING (...)" grouping.
     *
     * @return  self
     */
    public function orHavingClose() : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'orHavingClose',
            'args' => [],
        ];

        return $this;
    }

    /**
     * Start returning results after "OFFSET ..."
     *
     * @param integer $number starting result number
     *
     * @return  self
     */
    public function offset($number) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'offset',
            'args' => [$number],
        ];

        return $this;
    }

    /**
     * Enables the query to be cached for a specified amount of time.
     *
     * @param integer $lifetime number of seconds to cache
     *
     * @return  self
     */
    public function cached($lifetime = null) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'cached',
            'args' => [$lifetime],
        ];

        return $this;
    }

    /**
     * Set the value of a parameter in the query.
     *
     * @param string $param parameter key to replace
     * @param mixed  $value value to use
     *
     * @return  self
     */
    public function param($param, $value) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'param',
            'args' => [$param, $value],
        ];

        return $this;
    }

    /**
     * Adds "USING ..." conditions for the last created JOIN statement.
     *
     * @param mixed $columns column names
     * @param   ...
     *
     * @return  self
     */
    public function using(...$columns) : self
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = [
            'name' => 'using',
            'args' => $columns,
        ];

        return $this;
    }

    /**
     * Checks whether a column value is unique.
     * Excludes itself if loaded.
     *
     * @param string $field the field to check for uniqueness
     * @param mixed  $value the value to check for uniqueness
     *
     * @return  bool     whteher the value is unique
     */
    public function unique(string $field, $value) : bool
    {
        $model = self::factory(static::class)
                    ->where($field, '=', $value)
                    ->find();

        if ($this->loaded()) {
            return ( ! ($model->loaded() AND $model->pk() !== $this->pk()));
        }

        return ( ! $model->loaded());
    }

    /**
     * Quote Table
     *
     * @param string $orm_model Model Name
     *
     * @return string
     *
     * @throws Exception
     */
    public static function quoteTable(string $orm_model) : string
    {
        try
        {
            $table = Database::instance()->quoteTable(strtolower($orm_model));
        }
        catch (\Modseven\Exception|\Modseven\Database\Exception $e)
        {
            throw new Exception($e->getMessage(), null, $e->getCode(), $e);
        }

        return $table;
    }
}
