<?php
/**
 * ORM Behavior Class
 *
 * @copyright  (c) 2016-2018 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\ORM;

use Modseven\ORM\Behavior\Local;
use Modseven\Database\Query\Builder\Select;

class Behavior
{
    /**
     * Database query builder
     * @var Select;
     */
    protected Select $_config;

    /**
     * Creates and returns a new ORM behavior.
     *
     * @chainable
     *
     * @param string $behavior Behavior class name
     * @param array  $config   Parameter for find()
     *
     * @return  Behavior
     *
     * @throws Exception
     */
    public static function factory(string $behavior, ?array $config = null) : Behavior
    {
        if ($config !== null)
        {
            if ( ! is_callable($config))
            {
                throw new Exception('Behavior cannot be created: function does not exists');
            }

            // This is either a callback as an array or a lambda
            return new Local($config);
        }

        return new $behavior($config);
    }

    protected function __construct($config = null)
    {
        if ($config !== null)
        {
            $this->_config = $config;
        }
    }

    public function on_construct($model, $id): bool { return true; }

    public function on_create($model): void {}

    public function on_update($model): void {}
}
