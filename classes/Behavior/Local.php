<?php
/**
 * Local Behavior Class
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\ORM\Behavior;

use Modseven\ORM\ORM;
use Modseven\ORM\Behavior;

class Local extends Behavior
{
    /**
     * Callback to execute
     * @var callable
     */
    protected $_callback;

    /**
     * Constructs a behavior object
     * @param callable $callback Callback to execute
     */
    protected function __construct(callable $callback)
    {
        $this->_callback = $callback;
        parent::__construct(null);
    }

    /**
     * Constructs a new model and loads a record if given
     *
     * @param ORM   $model  The model
     * @param mixed $id     Parameter for find or object to load
     *
     * @return bool|mixed
     */
    public function onConstruct($model, $id) : bool
    {
        $params = ['construct', $id];
        $result = call_user_func_array($this->_callback, $params);

        if (is_bool($result))
        {
            return $result;
        }

        // Continue loading the record
        return true;
    }

    /**
     * The model is updated
     *
     * @param ORM $model The model
     */
    public function onUpdate($model) : void
    {
        $params = ['update'];
        call_user_func_array($this->_callback, $params);
    }

    /**
     * A new model is created
     *
     * @param ORM $model The model
     */
    public function onCreate($model) : void
    {
        $params = ['create'];
        call_user_func_array($this->_callback, $params);
    }
}
