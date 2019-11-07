<?php
/**
 * ORM Validation exceptions.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\ORM\Validation;

use Throwable;
use Modseven\Validation;

class Exception extends \Modseven\Exception
{
    /**
     * Array of validation objects
     * @var array
     */
    protected array $_objects = [];

    /**
     * The alias of the main ORM model this exception was created for
     * @var string
     */
    protected ?string $_alias = null;

    /**
     * Constructs a new exception for the specified model
     *
     * @param string     $alias    The alias to use when looking for error messages
     * @param Validation $object   The Validation object of the model
     * @param string     $message  The error message
     * @param array      $values   The array of values for the error message
     * @param integer    $code     The error code for the exception
     * @param Throwable  $previous The previous Exception
     *
     * @return void
     */
    public function __construct(
        $alias, Validation $object, $message = 'Failed to validate array', ?array $values = null, $code = 0,
        ?Throwable $previous = null
    ) {
        $this->_alias = $alias;
        $this->_objects['_object'] = $object;
        $this->_objects['_has_many'] = false;

        parent::__construct($message, $values, $code, $previous);
    }

    /**
     * Adds a Validation object to this exception
     *
     * @param string     $alias    The relationship alias from the model
     * @param Validation $object   The Validation object to merge
     * @param mixed      $has_many The array key to use if this exception can be merged multiple times
     *
     * @return self
     */
    public function addObject(string $alias, Validation $object, $has_many = false) : self
    {
        // We will need this when generating errors
        $this->_objects[$alias]['_has_many'] = ($has_many !== false);

        if ($has_many === true)
        {
            // This is most likely a has_many relationship
            $this->_objects[$alias][]['_object'] = $object;
        }
        elseif ($has_many)
        {
            // This is most likely a has_many relationship
            $this->_objects[$alias][$has_many]['_object'] = $object;
        }
        else
        {
            $this->_objects[$alias]['_object'] = $object;
        }

        return $this;
    }

    /**
     * Merges an ORM_Validation_Exception object into the current exception
     * Useful when you want to combine errors into one array
     *
     * @param self  $object   The exception to merge
     * @param mixed $has_many The array key to use if this exception can be merged multiple times
     *
     * @return self
     */
    public function merge(self $object, $has_many = false) : self
    {
        $alias = $object->alias();

        // We will need this when generating errors
        $this->_objects[$alias]['_has_many'] = ($has_many !== false);

        if ($has_many === true)
        {
            // This is most likely a has_many relationship
            $this->_objects[$alias][] = $object->objects();
        }
        elseif ($has_many)
        {
            // This is most likely a has_many relationship
            $this->_objects[$alias][$has_many] = $object->objects();
        }
        else
        {
            $this->_objects[$alias] = $object->objects();
        }

        return $this;
    }

    /**
     * Returns a merged array of the errors from all the Validation objects in this exception
     *
     * @param string $directory Directory to load error messages from
     * @param mixed  $translate Translate the message
     *
     * @return  array
     */
    public function errors(?string $directory = null, $translate = true) : array
    {
        return $this->generateErrors($this->_alias, $this->_objects, $directory, $translate);
    }

    /**
     * Recursive method to fetch all the errors in this exception
     *
     * @param string $alias     Alias to use for messages file
     * @param array  $array     Array of Validation objects to get errors from
     * @param string $directory Directory to load error messages from
     * @param mixed  $translate Translate the message
     *
     * @return array
     */
    protected function generateErrors(string $alias, array $array, ?string $directory, $translate) : array
    {
        $errors = [];

        foreach ($array as $key => $object)
        {
            if (is_array($object))
            {
                $errors[$key] = ($key === '_external')
                    // Search for errors in $alias/_external.php
                    ? $this->generateErrors($alias . '/' . $key, $object, $directory, $translate)
                    // Regular models get their own file not nested within $alias
                    : $this->generateErrors($key, $object, $directory, $translate);
            }
            elseif ($object instanceof Validation)
            {
                if ($directory === null) {
                    // Return the raw errors
                    $file = null;
                }
                else {
                    $file = trim($directory . '/' . $alias, '/');
                }

                // Merge in this array of errors
                $errors += $object->errors($file, $translate);
            }
        }

        return $errors;
    }

    /**
     * Returns the protected _objects property from this exception
     *
     * @return array
     */
    public function objects() : array
    {
        return $this->_objects;
    }

    /**
     * Returns the protected _alias property from this exception
     *
     * @return string
     */
    public function alias() : string
    {
        return $this->_alias;
    }
}