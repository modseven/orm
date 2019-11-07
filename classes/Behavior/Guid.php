<?php
/**
 * Guid Behavior Class
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\ORM\Behavior;

use Modseven\Arr;
use Modseven\Log;

use Modseven\ORM\ORM;
use Modseven\ORM\UUID;
use Modseven\ORM\Behavior;
use Modseven\ORM\Exception;
use Modseven\Database\DB;

class Guid extends Behavior
{
    /**
     * Table column for GUID value
     * @var string
     */
    protected string $_guid_column = 'guid';

    /**
     * Allow model creat on on guid key only
     * @var boolean
     */
    protected bool $_guid_only = true;

    /**
     * Verify GUID
     * @var boolean
     */
    protected bool $_guid_verify = false;

    /**
     * Constructs a behavior object
     *
     * @param array $config Configuration parameters
     */
    protected function __construct(array $config)
    {
        parent::__construct($config);

        $this->_guid_column = Arr::get($config, 'column', $this->_guid_column);
        $this->_guid_only = Arr::get($config, 'guid_only', $this->_guid_only);
        $this->_guid_verify = Arr::get($config, 'verify', $this->_guid_verify);
    }

    /**
     * Constructs a new model and loads a record if given
     *
     * @param ORM $model The model
     * @param mixed             $id    Parameter for find or object to load
     *
     * @return bool
     *
     * @throws Exception
     */
    public function onConstruct($model, $id): bool
    {
        if (($id !== null) && ! is_array($id) && ! ctype_digit($id))
        {
            if ( ! UUID::valid($id))
            {
                throw new Exception('Invalid UUID: :id', [':id' => $id]);
            }
            $model->where($this->_guid_column, '=', $id)->find();

            // Prevent further record loading
            return false;
        }

        return true;
    }

    /**
     * The model is updated, add a guid value if empty
     *
     * @param ORM $model The model
     *
     * @throws Exception
     */
    public function onUpdate($model) : void
    {
        $this->createGuid($model);
    }

    /**
     * Create GUID
     *
     * @param ORM $model    Model to generate GUID for
     *
     * @throws Exception
     */
    private function createGuid($model) : void
    {
        if ($this->_guid_verify === false)
        {
            $model->set($this->_guid_column, UUID::v4());
            return;
        }

        $current_guid = $model->get($this->_guid_column);

        // Try to create a new GUID
        $query = DB::select()->from($model->tableName())
                   ->where($this->_guid_column, '=', ':guid')
                   ->limit(1);

        while (empty($current_guid))
        {
            $current_guid = UUID::v4();

            $query->param(':guid', $current_guid);

            try
            {
                if ($query->execute()->get($model->primaryKey(), false) !== false)
                {
                    Log::instance()->notice('Duplicate GUID created for {table}', [
                        'table' => $model->tableName()
                    ]);
                    $current_guid = '';
                }
            }
            catch (\Modseven\Exception $e)
            {
                throw new Exception($e->getMessage(), null, $e->getCode(), $e);
            }
        }

        $model->set($this->_guid_column, $current_guid);
    }

    /**
     * A new model is created, add a guid value
     *
     * @param ORM $model The model
     *
     * @throws    Exception
     */
    public function onCreate($model) : void
    {
        $this->createGuid($model);
    }
}
